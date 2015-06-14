class Sorceror::CLI
  attr_accessor :options

  def subscribe
    Sorceror::Backend.start_subscriber(options[:args].first.try(:to_sym))
    Sorceror::Config.subscriber_threads.tap do |threads|
      print_status "[sorceror] Working [#{threads} thread#{'s' if threads > 1} per group]..."
    end
    sleep 0.2 until Sorceror::Backend.subscriber_stopped?
  end

  def parse_args(args)
    options = {}

    require 'optparse'
    parser = OptionParser.new do |opts|
      opts.banner = "Usage: sorceror [options] action"

      opts.separator ""
      opts.separator "Actions:"
      opts.separator "    sorceror subscribe"
      opts.separator ""
      opts.separator "Options:"

      opts.on "-l", "--require FILE", "File to require to load your app. Don't worry about it with rails" do |file|
        options[:require] = file
      end

      opts.on "-t", "--threads [NUM]", "Number of subscriber worker threads to run. Defaults to 10." do |threads|
        Sorceror::Config.subscriber_threads = threads.to_i
      end

      opts.on "-D", "--daemonize", "Daemonize process" do
        options[:daemonize] = true
      end

      opts.on "-P", "--pid-file [pid_file]", "Set a pid-file" do |pid_file|
        options[:pid_file] = pid_file
      end

      opts.on("-V", "--version", "Show version") do
        puts "Sorceror #{Sorceror::VERSION}"
        puts "License MIT"
        exit
      end
    end

    args = args.dup
    parser.parse!(args)

    options[:action] = args.shift.try(:to_sym)

    options[:args] = args

    case options[:action]
    when :subscribe           then raise "Why are you specifying a criteria?"   if     options[:criterias].present?
    else puts parser; exit 1
    end

    options
  rescue SystemExit
    exit
  rescue StandardError => e
    puts e
    exit
  end

  def load_app
    if options[:require]
      begin
        require options[:require]
      rescue LoadError
        require "./#{options[:require]}"
      end
    else
      require 'rails'
      require 'sorceror/railtie'
      require File.expand_path("./config/environment")
      ::Rails.application.eager_load!
    end
  end

  def boot
    self.options = parse_args(ARGV)
    daemonize if options[:daemonize]
    write_pid if options[:pid_file]
    load_app
    run
  end

  def daemonize
    Process.daemon(true)
  end

  def write_pid
    File.open(options[:pid_file], 'w') do |f|
      f.puts Process.pid
    end
  end

  def run
    trap_signals
    case options[:action]
    when :subscribe then subscribe
    end
  end

  def print_status(msg)
    Sorceror.info msg
    STDERR.puts msg
  end

  def trap_signals
    %w(SIGINT).each do |signal|
      Signal.trap(signal) do
        Thread.new do
          print_status "Shutting down..."
          Sorceror.disconnect
        end.join
        exit 0
      end
    end
  end
end
