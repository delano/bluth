
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
      Familia.ld "ENQUEUING: #{self} #{gob.jobid.short} to #{q}"
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
      Familia.ld "PERFORM: #{self.to_hash.inspect}"
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
      Familia.ld "Deleting #{self.jobid} from #{queue.rediskey}"
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
      Familia.ld "Moving #{self.jobid} from #{from.rediskey} to #{to.rediskey}"
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

