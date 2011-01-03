require 'benchmark'
require 'eventmachine'
require 'rufus/scheduler'
require 'daemonizing'

class Rufus::Scheduler::SchedulerCore
  # See lib/rufus/sc/scheduler.rb
  def handle_exception(task, exception)
    case exception
    when SystemExit
      exit
    else
      Familia.info exception.message
      Familia.info exception.backtrace
      task.unschedule
    end
  end
end

module Bluth
  @salt = rand.gibbler.shorten(10).freeze
  class << self
    attr_reader :salt
  end
  
  module WorkerBase
    
    def init(h=nil, u=nil, w=nil)
      @host, @user, @wid, = h || Bluth.sysinfo.hostname, u || Bluth.sysinfo.user, w
      @pid_file ||= "/tmp/#{name}.pid"
      @log_file ||= "/tmp/#{name}.log"
      @success ||= 0
      @failure ||= 0
      @problem ||= 0
    end
    
    def wid
      @wid ||= [host, user, rand, Time.now.to_f].gibbler.short
      @wid
    end
    
    # Used by daemonize as the process name (linux only)
    def name
      [self.class.prefix, wid].flatten.join '-'
    end
    
    #def rediskey(suffix=nil)
    #  self.class.rediskey index, suffix
    #end
    
    def current_job 
      Gibbler::Digest.new(@current_job || '')
    end
    
    def kill(force=false)
      if force || host == Bluth.sysinfo.hostname
        Familia.info "Destroying #{self.index} (this machine is: #{Bluth.sysinfo.hostname}; worker is: #{host})"
        Worker.kill self.pid_file if File.exists?(self.pid_file) rescue Errno::ESRCH
        File.delete self.log_file if File.exists?(self.log_file)
        destroy!
      else
        Familia.info "Worker #{self.index} not running on #{Bluth.sysinfo.hostname}"
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
        me = new nil, nil, wid
        super(me.index)
      end
      def run!(*args)
        me = new
        Familia.info "Created: #{me.rediskey}"
        me.run!
        me
      end
      def run(*args)
        me = new
        Familia.info "Created: #{me.rediskey}"
        me.run
        me
      end
      def kill(pid_file)
        self.class.runblock :onexit
        pid = read_pid_file pid_file
        super(pid_file, 10)
      end
      def onstart &blk
        @onstart = blk unless blk.nil?
        @onstart
      end
      def onexit &blk
        @onexit = blk unless blk.nil?
        @onexit
      end
      # A convenience method for calling onstart/onexit blocks
      def runblock meth
        blk = self.send(meth)
        return if blk.nil?
        instance_eval &blk
      end
    end
  end
  
  class Worker < Storable
    @interval = 2 #.seconds
    class << self
      attr_accessor :interval
    end
    include WorkerBase
    include Familia
    include Logging
    include Daemonizable
    prefix [:bluth, :worker]
    index [:host, :user, :wid]
    field :host
    field :user
    field :wid
    field :process_id => Integer
    field :pid_file
    field :log_file
    field :current_job
    field :success => Integer
    field :failure => Integer
    field :problem => Integer
    include Familia::Stamps
    [:success, :failure, :problem].each do |name|
      define_method "#{name}!" do
        v = self.send(name) + 1
        self.send :"#{name}=", v
        self.instance_variable_set '@current_job', ''
        update_time!  # calls save
      end
    end
    
    def run!
      begin
        self.class.runblock :onstart
        find_gob
      rescue => ex
        msg = "#{ex.class}: #{ex.message}"
        Familia.info msg
        Familia.trace :EXCEPTION, msg, caller[1] if Familia.debug?
        self.class.runblock :onexit
        destroy!
      rescue Interrupt => ex
        puts $/, "Exiting..."
        self.class.runblock :onexit
        destroy!
      end
    end
    
    def run
      begin
        @process_id = $$
        scheduler = Rufus::Scheduler.start_new
        self.class.runblock :onstart
        Familia.info "Setting interval: #{Worker.interval} sec (queuetimeout: #{Bluth.queuetimeout})"
        Familia.reconnect_all! # Need to reconnect after daemonize
        save
        scheduler.every Worker.interval, :blocking => true do |task|
          Familia.ld "#{$$} TICK @ #{Time.now.utc}"
          sleep rand
          find_gob task
        end
        scheduler.join
            
      rescue => ex
        msg = "#{ex.class}: #{ex.message}"
        Familia.info msg
        Familia.trace :EXCEPTION, msg, caller[1] if Familia.debug?
        self.class.runblock :onexit
        destroy!
      rescue Interrupt => ex
        puts <<-EOS.gsub(/(?:^|\n)\s*/, "\n")
          Exiting...
          (You may need to wait up to #{Bluth.queuetimeout} seconds
          for this worker to exit cleanly.)
        EOS
        # We reconnect to the queue in case we're currently
        # waiting on a brpop (blocking pop) timeout.
        self.class.runblock :onexit
        destroy!
      end
      
    end
    
    
    private 
    
    # DO NOT return from this method
    def find_gob(task=nil)
      begin
        job = Bluth.pop
        unless job.nil?
          job.wid = self.wid
          if job.delayed?
            job.attempts = 0
            job.retry!
          elsif !job.attempt?
            job.failure! "Too many attempts"
          else
            job.stime = Time.now.utc.to_i
            self.working! job.jobid
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
    include Familia
    include Logging
    include Daemonizable
    @interval = 20
    @timeout = 60 #not working
    @every = []
    class << self
      attr_accessor :interval, :timeout, :schedule
      def interval(v=nil)
        @interval = v unless v.nil?
        @interval
      end
      def every interval=nil, opts={}, &blk
        unless interval.nil?
          @every << [interval, opts, blk]
        end
        @every
      end
    end
    prefix [:bluth, :scheduler]
    index [:host, :user, :wid]
    field :host
    field :user
    field :wid
    field :process_id => Integer
    field :pid_file
    field :log_file
    include Familia::Stamps
    
    def run!
      run
    end
    
    def run
      begin
        EM.run {
          @process_id = $$
          srand(Bluth.salt.to_i(16) ** @process_id)
          self.class.runblock :onstart
          Familia.info "Setting interval: #{Worker.interval} sec (queuetimeout: #{Bluth.queuetimeout})"
          Familia.reconnect_all! # Need to reconnect after daemonize
          raise Familia::Problem, "Only 1 scheduler at a time" if !ScheduleWorker.instances.empty?
          save # persist and make note the scheduler is running
          ScheduleWorker.schedule = Rufus::Scheduler::EmScheduler.start_new
          self.class.every.each do |args|
            interval, opts, blk = *args
            Familia.ld " scheduling every #{interval}: #{opts}"
            ScheduleWorker.schedule.every interval, opts, &blk
          end
        }
      rescue => ex
        msg = "#{ex.class}: #{ex.message}"
        puts msg
        Familia.info ex.backtrace
        Familia.trace :EXCEPTION, msg, caller[1] if Familia.debug?
        self.class.runblock :onexit
        destroy!
      rescue Interrupt => ex
        puts $/, "Exiting..."
        self.class.runblock :onexit
        destroy!
      end
    end
    
  end
  
  Bluth.scheduler = Bluth::ScheduleWorker
end

