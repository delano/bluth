require 'eventmachine'
require 'rufus/scheduler'
require 'daemonizing'
require 'timeout'

module Bluth
  @salt = rand.gibbler.shorten(10).freeze
  class << self
    attr_reader :salt
  end
  
  module WorkerBase

    def id
      @id ||= [host, user, rand, Time.now].gibbler.short
    end
    
    def longid
      [host, user, id].join('-')
    end
    
    # Used by daemonize as the process name (linux only)
    def name
      "bs-#{self.class.prefix}-#{id}"
    end
    
    def key(suffix=nil)
      self.class.key longid, suffix
    end
    
    def initialize
      @host, @user = Bluth.sysinfo.hostname, Bluth.sysinfo.user
      @pid_file ||= "/tmp/#{self.class.prefix}-#{id}.pid"
      @log_file ||= "/tmp/#{self.class.prefix}-#{id}.log"
      @success, @failure, @problem = 0, 0, 0
    end
    
    def current_job 
      Gibbler::Digest.new(@current_job || '')
    end
    
    def kill(force=false)
      if force || host == Bluth.sysinfo.hostname
        STDERR.puts "Destroying #{self.index} (this machine is: #{Bluth.sysinfo.hostname}; worker is: #{host})"
        Worker.kill self.pid_file if File.exists?(self.pid_file) rescue Errno::ESRCH
        File.delete self.log_file if File.exists?(self.log_file)
        destroy!
      else
        STDERR.puts "Worker #{self.index} not running on #{Bluth.sysinfo.hostname}"
      end
    end
    
    def working! gobid
      @current_job = gobid
      update_time
      save
    end
    
    def self.included(obj)
      obj.extend WorkerBase::ClassMethods
    end
    
    module ClassMethods
      def from_redis(wid)
        me = new 
        me.id = wid
        super(me.longid)
      end

      def run!(*args)
        me = new
        Familia.info "Created: #{me.key}"
        me.run!
        me
      end

      def run(*args)
        me = new
        Familia.info "Created: #{me.key}"
        me.run
        me
      end
      
      def kill(pid_file)
        pid = read_pid_file pid_file
        super(pid_file, 10)
      end
      
      
    end
    
  end
  
  class Worker < Storable
    include WorkerBase
    @interval = 2.seconds
    class << self
      attr_accessor :interval
    end
    include Familia
    include Logging
    include Daemonizable
    prefix :worker
    index :id
    field :host
    field :user
    field :id
    field :process_id => Integer
    field :pid_file
    field :log_file
    field :current_job
    field :success => Integer
    field :failure => Integer
    field :problem => Integer
    include Familia::Stamps
    def success!
      @success += 1
      @current_job = ""
      update_time
      save
    end
    def failure!
      @failure += 1
      @current_job = ""
      update_time
      save
    end
    def problem!
      @problem += 1
      @current_job = ""
      update_time
      save
    end
    
    def run!
      begin
        find_gob
      rescue => ex
        msg = "#{ex.class}: #{ex.message}"
        STDERR.puts msg
        Familia.ld :EXCEPTION, msg, caller[1] if Familia.debug?
        destroy!
      rescue Interrupt => ex
        puts $/, "Exiting..."
        destroy!
      end
    end
    
    def run
      begin
        @process_id = $$
        save
      
        scheduler = Rufus::Scheduler.start_new
        Familia.info "Setting interval: #{Worker.interval} sec (poptimeout: #{Bluth.poptimeout})"
        Familia.reconnect_all! # Need to reconnect after daemonize
        ## TODO: Works but needs to restart scheduler
        ##Signal.trap("USR1") do
        ##  Worker.interval += 1
        ##  Familia.info "Setting interval: #{Worker.interval} sec"
        ##end
        ##Signal.trap("USR2") do
        ##  Worker.interval -= 1
        ##  Familia.info "Setting interval: #{Worker.interval}"
        ##end
        scheduler.every Worker.interval, :blocking => true do |task|
          Familia.ld "#{$$} TICK @ #{Time.now.utc}"
          sleep rand
          find_gob task
        end
        scheduler.join
            
      rescue => ex
        msg = "#{ex.class}: #{ex.message}"
        STDERR.puts msg
        Familia.ld :EXCEPTION, msg, caller[1] if Familia.debug?
        destroy!
      rescue Interrupt => ex
        puts <<-EOS.gsub(/(?:^|\n)\s*/, "\n")
          Exiting...
          (You may need to wait up to #{Bluth.poptimeout} seconds
          for this worker to exit cleanly.)
        EOS
        # We reconnect to the queue in case we're currently
        # waiting on a brpop (blocking pop) timeout.
        destroy!
      end
      
    end
    
    
    private 
    require 'benchmark'
    # DO NOT return from this method
    def find_gob(task=nil)
      begin
        job = Bluth.pop
        unless job.nil?
          job.wid = self.id
          if job.delayed?
            job.attempts = 0
            job.retry!
          elsif !job.attempt?
            job.failure! "Too many attempts"
          else
            job.stime = Time.now.utc.to_i
            self.working! job.id
            tms = Benchmark.measure do
              job.perform
            end
            job.cpu = [tms.utime.fineround(3),tms.stime.fineround(3),tms.real.fineround(3)]
            job.save
            job.success!
            self.success!
          end
        end   
      rescue Bluth::Shutdown => ex
        msg = "Shutdown requested: #{ex.message}"
        job.success! msg
        Familia.info msg
        task.unschedule
        destroy!
        exit
      rescue Bluth::Maeby => ex
        Familia.info ex.message
        job.success! ex.message
        self.success!
      rescue Bluth::Buster => ex  
        Familia.info ex.message
        job.failure! ex.message
        self.failure!
      rescue => ex
        Familia.info ex.message
        Familia.info ex.backtrace
        job.retry! "#{ex.class}: #{ex.message}" if job
        problem!
        #if problem > 5
        #  ## TODO: SEND EMAIL
        #  task.unschedule unless task.nil? # Kill this worker b/c something is clearly wrong
        #  destroy!
        #  EM.stop
        #  exit 1
        #end
      end
    end

  end
  
  class ScheduleWorker < Storable
    include WorkerBase
    @interval = 20
    @timeout = 60 #not working
    class << self
      attr_accessor :interval, :timeout
      def interval(v=nil)
        @interval = v unless v.nil?
        @interval
      end
    end
    include Familia
    include Logging
    include Daemonizable
    prefix :scheduler
    index :id
    field :host
    field :user
    field :id
    field :process_id => Integer
    field :pid_file
    field :log_file
    field :scheduled => Integer
    field :monitored => Integer
    field :timeouts => Integer
    include Familia::Stamps
    attr_reader :schedule
    attr_reader :monitors

    def scheduled!(count=1)
      @scheduled ||= 0
      @scheduled += count
      update_time
      save
    end
    def monitored!(count=1)
      @monitored ||= 0
      @monitored += count
      update_time
      save
    end
    def timeout!(count=1)
      @timeouts ||= 0
      @timeouts += count
      update_time
      save
    end
    def run!
      run
    end
    def run
      begin
        raise Familia::Problem, "Only 1 scheduler at a time" if ScheduleWorker.any?
        
        EM.run {
          @process_id = $$
          srand(Bluth.salt.to_i(16) ** @process_id)
          @schedule = Rufus::Scheduler::EmScheduler.start_new
          save # persist and make note the scheduler is running
          prepare
          @schedule.every self.class.interval, :tags => :keeper do |keeper_task|
            begin
              scheduled_work(keeper_task)
            rescue => ex
              msg = "#{ex.class}: #{ex.message}"
              STDERR.puts msg
              STDERR.puts ex.backtrace
              Familia.ld :EXCEPTION, msg, caller[1] if Familia.debug?
            end
            sleep rand  # prevent thrashing
          end
        }
      rescue => ex
        msg = "#{ex.class}: #{ex.message}"
        puts msg
        STDERR.puts ex.backtrace
        Familia.ld :EXCEPTION, msg, caller[1] if Familia.debug?
        destroy!
      rescue Interrupt => ex
        puts $/, "Exiting..."
        destroy!
      end
    end
    
    protected
  
    def prepare
    end
    
    def scheduled_work(keeper)
      STDOUT.puts "Come on!"
    end
    
  end
  
end

class Rufus::Scheduler::SchedulerCore
  # See lib/rufus/sc/scheduler.rb
  def handle_exception(job, exception)
    case exception
    when SystemExit
      exit
    else
      super
    end
  end
end
