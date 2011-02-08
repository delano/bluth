# encoding: utf-8
BLUTH_LIB_HOME = File.expand_path File.dirname(__FILE__) unless defined?(BLUTH_LIB_HOME)

local_libs = %w{familia}
local_libs.each { |dir| 
  a = File.join(BLUTH_LIB_HOME, '..', '..', dir, 'lib')
  $:.unshift a
}

require 'sysinfo'
require 'storable'
require 'gibbler'
require 'familia'

module Bluth
  module VERSION
    def self.to_s
      load_config
      [@version[:MAJOR], @version[:MINOR], @version[:PATCH]].join('.')
    end
    alias_method :inspect, :to_s
    def self.load_config
      require 'yaml'
      @version ||= YAML.load_file(File.join(BLUTH_LIB_HOME, '..', 'VERSION.yml'))
    end
  end
end

module Bluth   
  # A fatal error. Gob fails.
  class Buster < Familia::Problem; end
  # A non-fatal error. Gob succeeds.
  class Maeby < Familia::Problem; end
  # A shutdown request. We burn down the banana stand.
  class Shutdown < Familia::Problem; end
  @db = 0
  @env = :dev
  @queuetimeout = 60 #.seconds
  @handlers = []
  @locks = []
  @sysinfo = nil
  @priority = []
  @scheduler = nil 
  class << self
    attr_reader :handlers, :db, :conf, :locks
    attr_accessor :redis, :uri, :priority, :scheduler, :queuetimeout, :env
    def sysinfo
      @sysinfo ||= SysInfo.new.freeze
      @sysinfo 
    end
    # A block to be called before a worker starts.
    # 
    # e.g.
    #      Bluth.onconnect do
    #        config = YourProject.load_config
    #        Familia.uri = config[:redis_uri]
    #      end
    #
    # Note:
    # this block can be called multiple times so do not
    # put anything with side effects in here.
    def onconnect &blk
      @onconnect = blk unless blk.nil?
      @onconnect
    end
    def connect
      instance_eval &onconnect unless onconnect.nil?
    end
  end
  
  def Bluth.clear_locks
    @locks.each { |lock| 
      Familia.info "Removing lock #{lock}"
      Bluth.redis.del lock 
    }
  end
  def Bluth.find_locks
    @locks = Bluth.redis.keys(Familia.rediskey('*', :lock))
  end
  
  def Bluth.queue?(n)
    Bluth::Queue.queues.collect(&:name).member?(n.to_s.to_sym)
  end
  def Bluth.queue(n)
    raise ArgumentError, "No such queue: #{n}" unless queue?(n)
    Bluth::Queue.send n
  end
  
  require 'bluth/worker'
  
  module Queue  # if this is a module the 
    include Familia
    prefix [:bluth, :queue]
    class_list :critical #, :class => Bluth::Gob
    class_list :high
    class_list :low
    class_list :running
    class_list :successful
    class_list :failed
    class_list :orphaned
    class << self
      # The complete list of queues in the order they were defined
      def queues
        Bluth::Queue.class_lists.collect(&:name).collect do |qname|
          self.send qname
        end
      end
      # The subset of queues that new jobs arrive in, in order of priority
      def entry_queues
        Bluth.priority.collect { |qname| self.send qname }
      end
    end
    
    # Set default priority
    Bluth.priority = [:critical, :high, :low]
  end
  
  # Workers use a blocking pop and will wait for up to 
  # Bluth.queuetimeout (seconds) before returnning nil. 
  # Note that the queues are still processed in order. 
  # If all queues are empty, the first one to return a
  # value is use. See: 
  #
  # http://code.google.com/p/redis/wiki/BlpopCommand
  def Bluth.shift
    blocking_queue_handler :blpop
  end
  
  def Bluth.pop
    blocking_queue_handler :brpop
  end
  
  private
  
  # +meth+ is either :blpop or :brpop
  def Bluth.blocking_queue_handler meth
    gob = nil
    begin
      order = Bluth::Queue.entry_queues.collect(&:rediskey)
      order << Bluth.queuetimeout  # We do it this way to support Ruby 1.8
      queue, gobid = *(Bluth::Queue.redis.send(meth, *order) || [])
      unless queue.nil?
        Familia.ld "FOUND #{gobid} id #{queue}" if Familia.debug?
        gob = Gob.from_redis gobid
        raise Bluth::Buster, "No such gob object: #{gobid}" if gob.nil?
        Bluth::Queue.running << gob.jobid
        gob.current_queue = :running
        gob.save
      end
    rescue => ex
      if queue.nil?
        Familia.info "ERROR: #{ex.message}"
      else
        Familia.info "ERROR (#{ex.message}): #{gobid} is an orphan"
        Bluth::Queue.orphaned << gobid
      end
      Familia.ld ex.backtrace if Familia.debug?
    end
    gob
  end  
end



module Bluth
  module Handler
    
    def self.extended(obj)
      obj.send :include, Familia
      obj.class_string :success
      obj.class_string :failure
      obj.class_string :running
      Bluth.handlers << obj
    end
    
    [:success, :failure, :running].each do |name| 
      define_method "#{name}!" do
        self.send(name).increment
      end
    end
    
    def enqueue(data={},q=nil)
      q = self.queue(q)
      gob = Gob.create generate_id(data), self, data
      gob.current_queue = q.name
      gob.created
      gob.attempts = 0
      gob.save
      Familia.ld "ENQUEUING: #{self} #{gob.jobid.short} to #{q}" if Familia.debug?
      q << gob.jobid
      gob
    end
    def queue(name=nil)
      @queue = name if name
      Bluth::Queue.send(@queue || :high)
    end
    def generate_id(*args)
      [self, Process.pid, Bluth.sysinfo.hostname, Time.now.to_f, *args].gibbler
    end
    def all
      Bluth::Gob.instances.select do |gob|
        gob.handler == self
      end
    end
    def prepare
    end
    
  end
end


module Bluth
  class Gob < Storable
    MAX_ATTEMPTS = 3.freeze unless defined?(Gob::MAX_ATTEMPTS)
    include Familia
    prefix [:bluth, :gob]
    ttl 3600 #.seconds
    index :jobid
    field :jobid => Gibbler::Digest
    field :handler => String
    field :data => Hash
    field :messages => Array
    field :attempts => Integer
    field :create_time => Float
    field :stime => Float
    field :etime => Float
    field :current_queue => Symbol
    field :thread_id => Integer
    field :cpu => Array
    field :wid => Gibbler::Digest
    include Familia::Stamps
    
    def jobid
      Gibbler::Digest.new(@jobid)
    end
    def clear!
      @attempts = 0
      @messages = []
      save
    end 
    def preprocess
      @attempts ||= 0
      @messages ||= []
      @create_time ||= Time.now.utc.to_f
    end
    def attempt?
      attempts < MAX_ATTEMPTS
    end
    def attempt!
      @attempts = attempts + 1
    end
    def current_queue
      @current_queue
    end
    def handler
      eval "::#{@handler}" if @handler
    end
    def perform
      @attempts += 1
      Familia.ld "PERFORM: #{self.to_hash.inspect}" if Familia.debug?
      @stime = Time.now.utc.to_f
      save # update the time
      self.handler.prepare if self.class.respond_to?(:prepare)
      self.handler.perform @data
      @etime = Time.now.utc.to_f
      save # update the time
    end
    def delayed?
      start = @stime || 0
      start > Time.now.utc.to_f
    end
    def retry!(msg=nil) 
      move! :high, msg
    end
    def failure!(msg=nil)
      @etime = Time.now.utc.to_i
      self.handler.failure!
      move! :failed, msg
    end
    def success!(msg=nil)
      @etime = Time.now.utc.to_i
      self.handler.success!
      move! :successful, msg
    end
    def duration
      return 0 if @stime.nil?
      et = @etime || Time.now.utc.to_i
      et - @stime
    end
    def queue
      Bluth.queue(current_queue)
    end
    def dequeue!
      Familia.ld "Deleting #{self.jobid} from #{queue.rediskey}" if Familia.debug?
      queue.remove 0, self.jobid
    end
    def running!
      move! :running
    end
    def move!(to, msg=nil)
      @thread_id = $$
      #if to.to_s == current_queue.to_s
      #  raise Bluth::Buster, "Cannot move job to the queue it's in: #{to}"
      #end
      from, to = Bluth.queue(current_queue), Bluth.queue(to)
      Familia.ld "Moving #{self.jobid} from #{from.rediskey} to #{to.rediskey}" if Familia.debug?
      @messages << msg unless msg.nil? || msg.empty?
      # We push first to make sure we never lose a Gob ID. Instead
      # there's the small chance of a job ID being in two queues. 
      to << @jobid
      ret = from.remove @jobid, 0
      @current_queue = to.name
      save # update messages
    end
  end
  
end

