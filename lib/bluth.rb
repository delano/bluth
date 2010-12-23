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
  def Bluth.find_locks
    @locks = Bluth.redis.keys(Familia.rediskey('*', :lock))
  end
  
  def Bluth.queue?(n)
    @queues.has_key?(n.to_sym)
  end
  def Bluth.queue(n)
    @queues[n.to_sym]
  end
  
  require 'bluth/gob'
  require 'bluth/worker'
  
  class Queue
    include Familia
    prefix :queue
    class_list :critical
    class_list :high
    class_list :low
    class_list :running
    class_list :failed
    class_list :orphaned
    class << self
      def queues
        Bluth::Queue.class_lists.collect(&:name)
      end
    end
    
    # Use the natural order of the defined lists
    Bluth.priority = Bluth::Queue.class_lists.collect(&:name)
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
      order = Bluth.priority.collect { |queue| queue.rediskey }
      order << Bluth.poptimeout  # We do it this way to support Ruby 1.8
      gobinfo = Bluth::Queue.redis.brpop *order
      unless gobinfo.nil?
        Familia.info "FOUND #{gobinfo.inspect}" if Familia.debug?
        gob = Gob.from_redis gobinfo[1]
        raise Bluth::Buster, "No such gob object: #{gobinfo[1]}" if gob.nil?
        Bluth::Running.push gob.jobid
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
  
  
  
  
end

