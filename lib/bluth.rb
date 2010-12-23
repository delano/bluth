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
  
  module Queue  # if this is a module the 
    include Familia
    prefix [:bluth, :queue]
    class_list :critical #, :include => Bluth::Queue::ListMethods
    class_list :high
    class_list :low
    class_list :running
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
  # Bluth.poptimeout (seconds) before returnning nil. 
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
      order << Bluth.poptimeout  # We do it this way to support Ruby 1.8
      queue, gobid = *(Bluth::Queue.redis.send(meth, *order) || [])
      unless queue.nil?
        Familia.ld "FOUND #{gobid} id #{queue}"
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
      Familia.ld ex.backtrace
    end
    gob
  end
  
  
end

