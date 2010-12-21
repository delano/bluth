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
  
  @db = 15
  @queues = {}
  @poptimeout = 60 #.seconds
  @handlers = []
  @locks = []
  @sysinfo = nil
  @priority = []
  @scheduler = nil 
  class << self
    attr_reader :queues, :handlers, :db, :conf, :locks
    attr_accessor :redis, :uri, :priority, :scheduler, :poptimeout
    def sysinfo
      @sysinfo ||= SysInfo.new.freeze
      @sysinfo 
    end
  end
  
  def Bluth.clear_locks
    @locks.each { |lock| 
      Familia.info "Removing lock #{lock}"
      Bluth.redis.del lock 
    }
  end
  
  def Bluth.queue?(n)
    @queues.has_key?(n.to_sym)
  end
  def Bluth.queue(n)
    @queues[n.to_sym]
  end
  
  def Bluth.conf=(conf={})
    @conf = conf.clone
    @conf[:db] = @db
    connect!
    @conf
  end
  
  def Bluth.connect!
    @uri = Redis.uri(@conf).freeze
    @redis = Familia.connect @uri
  end
  
  def Bluth.find_locks
    @locks = Bluth.redis.keys(Familia.key('*', :lock))
  end
  
  class Queue
    include Familia
    prefix :queue
    def self.rangeraw(count=100)
      gobids = Queue.redis.lrange(key, 0, count-1) || []
    end
    def self.range(count=100)
      gobids = rangeraw count
      gobids.collect { |gobid| 
        gob = Gob.from_redis gobid 
        next if gob.nil?
        gob.current_queue = self
        gob
      }.compact
    end
    def self.dequeue(gobid)
      Queue.redis.lrem key, 0, gobid
    end
    def self.inherited(obj)
      obj.prefix self.prefix
      obj.suffix obj.to_s.split('::').last.downcase.to_sym
      raise Buster.new("Duplicate queue: #{obj.suffix}") if Bluth.queue?(obj.suffix)
      Bluth.queues[obj.suffix] = obj
      super(obj)
    end
    def self.key(pref=nil,suff=nil)
      Familia.key( pref || prefix, suff || suffix)
    end
    def self.report
      Bluth.queues.keys.collect { |q| 
        klass = Bluth.queue(q)
        ("%10s: %4d" % [q, klass.size]) 
      }.join($/)
    end
    def self.from_string(str)
      raise Buster, "Unknown queue: #{str}" unless Bluth.queue?(str)
      Bluth.queue(str)
    end
    def self.any?
      size > 0
    end

    def self.empty?
      size == 0
    end

    def self.size
      begin
        Queue.redis.llen key
      rescue => ex
        STDERR.puts ex.message, ex.backtrace
        0
      end
    end 
    def self.push(gobid)
      Queue.redis.lpush self.key, gobid
    end
    
    def self.pop
      gobid = Queue.redis.rpoplpush key, Bluth::Running.key
      return if gobid.nil?
      Familia.ld "FOUND gob #{gobid} from #{self.key}"
      gob = Gob.from_redis gobid
      if gob.nil?
        Familia.info "No such gob object: #{gobid}" 
        Bluth::Running.dequeue gobid
        return
      end
      gob.current_queue = Bluth::Running
      gob.save
      gob
    end
  end
  
  # Workers use a blocking pop and will wait for up to 
  # Bluth.poptimeout (seconds) before returnning nil. 
  # Note that the queues are still processed in order. 
  # If all queues are empty, the first one to return a
  # value is use. See: 
  #
  # http://code.google.com/p/redis/wiki/BlpopCommand
  def Bluth.pop
    #Bluth.priority.each { |queue| 
    #  ret = queue.pop
    #  return ret unless ret.nil?
    #}
    begin
      #Familia.ld :BRPOP, Queue.redis, self, caller[1] if Familia.debug?
      order = Bluth.priority.collect { |queue| queue.key }
      order << Bluth.poptimeout  # We do it this way to support Ruby 1.8
      gobinfo = Bluth::Queue.redis.brpop *order
      unless gobinfo.nil?
        Familia.info "FOUND #{gobinfo.inspect}" if Familia.debug?
        gob = Gob.from_redis gobinfo[1]
        raise Bluth::Buster, "No such gob object: #{gobinfo[1]}" if gob.nil?
        Bluth::Running.push gob.id
        gob.current_queue = Bluth::Running
        gob.save
      end
    rescue => ex
      if gobinfo.nil?
        Familia.info "ERROR: #{ex.message}"
      else
        Familia.info "ERROR (#{ex.message}); putting #{gobinfo[1]} back on queue"
        Bluth::Orphaned.push gobinfo[1]
      end
    end
    gob
  end
  
  class Critical < Queue
  end
  class High < Queue
  end
  class Low < Queue
  end
  class Running < Queue
  end
  class Failed < Queue
  end
  class Successful < Queue
  end
  class Scheduled < Queue
  end
  class Orphaned < Queue
  end
    
  require 'bluth/gob'
  require 'bluth/worker'
  
  Bluth.priority = [Bluth::Critical, Bluth::High, Bluth::Low]
  Bluth.scheduler = ScheduleWorker
  
end

