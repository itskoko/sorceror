class Sorceror::Worker
  def start
    num_threads = Sorceror::Config.subscriber_threads
    Sorceror::Config.subscriber_topics.each do |topic|
      @distributor_threads ||= num_threads.times.map { DistributorThread.new(topic) }
      Sorceror.debug "[distributor] Starting #{num_threads} thread#{'s' if num_threads>1} topic:#{topic}"
    end
  end

  def stop
    return unless @distributor_threads
    Sorceror.debug "[distributor] Stopping #{@distributor_threads.count} threads"

    @distributor_threads.each { |distributor_thread| distributor_thread.stop }
    @distributor_threads = nil
  end

  def show_stop_status(num_requests)
    @distributor_threads.to_a.each { |distributor_thread| distributor_thread.show_stop_status(num_requests) }
  end

  class DistributorThread
    def initialize(topic)
      @stop = false
      @thread = Thread.new(topic) {|t| main_loop(t) }

      Sorceror.debug "[distributor] Subscribing to topic:#{topic} [#{@thread.object_id}]"
    end

    def subscribe(options)
      raise "No topic specified" unless options[:topic]

      @consumer = ::Poseidon::ConsumerGroup.new(Sorceror::Config.app,
                                                Sorceror::Config.kafka_hosts,
                                                Sorceror::Config.zookeeper_hosts,
                                                options[:topic],
                                                :trail => Sorceror::Config.test_mode,
                                                :max_wait_ms => 10)
    end

    def fetch_and_process_messages
      @consumer.fetch(:commit => false) do |partition, payloads|
        payloads.each do |payload|
          Sorceror.debug "[kafka] [receive] #{payload.value} topic:#{@consumer.topic} offset:#{payload.offset} parition:#{partition} #{Thread.current.object_id}"
          begin
            puts "Payload: #{payload.value}"
            # metadata = MetaData.new(@consumer, partition, payload.offset)
            # msg = Sorceror::Subscriber::Message.new(payload.value, :metadata => metadata)
            # msg.process
          rescue StandardError => e
            Sorceror.warn "[kafka] [receive] cannot process message: #{e}\n#{e.backtrace.join("\n")}"
            Sorceror::Config.error_notifier.call(e)
          end
        end
      end
    end

    def main_loop(topic)
      @consumer = subscribe(:topic => topic)

      while not @stop do
        begin
          fetch_and_process_messages
        rescue Poseidon::Connection::ConnectionFailedError
          Sorceror.debug "[kafka] Reconnecting... [#{@thread.object_id}]"
          @consumer = subscribe(@topic)
        end
        sleep 0.1
      end
      @consumer.close if @consumer
      @consumer = nil
    end

    def stop
      Sorceror.debug "[distributor] stopping status:#{@thread.status} [#{@thread.object_id}]"

      # We wait in case the consumer is responsible for more than one partition
      # see: https://github.com/bsm/poseidon_cluster/blob/master/lib/poseidon/consumer_group.rb#L229
      @stop = true
      @thread.join
      Sorceror.debug "[distributor] stopped [#{@thread.object_id}]"
    end

    def show_stop_status(num_requests)
      backtrace = @thread.backtrace

      STDERR.puts "Still processing messages (#{num_requests})"

      if num_requests > 1 && backtrace
        STDERR.puts
        STDERR.puts backtrace.map { |line| "  \e[1;30m#{line}\e[0m\n" }
        STDERR.puts
        STDERR.puts "I'm a little busy, check out my stack trace."
        STDERR.puts "Be patient (or kill me with -9, but that wouldn't be very nice of you)."
      else
        STDERR.puts "Just a second..."
      end
    end

    class MetaData
      def initialize(consumer, partition, offset)
        @consumer = consumer
        @partition = partition
        @offset = offset

        Sorceror.debug "[kafka] [metadata] topic:#{@consumer.topic} offset:#{offset} partition:#{partition}"
      end

      def ack
        Sorceror.debug "[kafka] [commit] topic:#{@consumer.topic} offset:#{@offset+1} partition:#{@partition}"
        @consumer.commit(@partition, @offset+1)
      end
    end
  end
end
