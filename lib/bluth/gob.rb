

module Bluth
  
  class Gob < Storable
    MAX_ATTEMPTS = 3.freeze unless defined?(Gob::MAX_ATTEMPTS)
    include Familia
    prefix [:bluth, :gob]
    ttl 3600 #.seconds
    index :jobid
    field :jobid => Gibbler::Digest
    field :kind => String
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
    
    [:success, :failure, :running].each do |name|
      class_string name
      define_method "#{name}!" do
        self.send(name).increment
        update_time!
      end
    end
    
    def self.inherited(obj)
      obj.extend Bluth::Gob::ClassMethods
      obj.prefix [:job, obj.to_s.split('::').last.downcase].join(':')
      Bluth.handlers << obj
    end
    
    module ClassMethods
      def clear
        keys.each do |key|
          Gob.redis.del key
        end
      end
      def enqueue(data={},q=nil)
        q = self.queue(q)
        job = Gob.create generate_id(data), self, data
        job.current_queue = q.name
        job.created
        job.attempts = 0
        job.save
        Familia.ld "ENQUEUING: #{self} #{job.jobid.short} to #{q}"
        q << job.jobid
        job
      end
      def queue(name=nil)
        @queue = name if name
        Bluth::Queue.send(@queue || :high)
      end
      def generate_id(*args)
        a = [self, Process.pid, Bluth.sysinfo.hostname, Time.now.to_f, *args]
        a.gibbler
      end
      def all
        Bluth::Gob.all.select do |job|
          job.kind == self
        end
      end
      def size
        all.size
      end
      def lock_key
        Familia.rediskey(prefix, :lock)
      end
      def lock!
        raise Bluth::Buster, "#{self} is already locked!" if locked?
        Familia.info "Locking #{self}"
        ret = Bluth::Gob.redis.set lock_key, 1
        Bluth.locks << lock_key
        ret == 'OK'
      end
      def unlock!
        Familia.info "Unlocking #{self}"
        ret = Bluth::Gob.redis.del lock_key
        Bluth.locks.delete lock_key
        ret
      end
      def locked?
        Bluth::Gob.redis.exists lock_key
      end
      def prepare
      end
      
    end
    
    def jobid
      @jobid = Gibbler::Digest.new(@jobid) if String === @jobid
      @jobid
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
    def kind
      @kind = eval "::#{@kind}" rescue @kind if @kind.is_a?(String)
      @kind
    end
    def kind=(v)
      @kind = v
    end
    def perform
      @attempts += 1
      Familia.ld "PERFORM: #{self.to_hash.inspect}"
      @stime = Time.now.utc.to_f
      save # update the time
      self.kind.prepare if self.class.respond_to?(:prepare)
      self.kind.perform @data
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
      self.kind.failure!
      move! :failed, msg
    end
    def success!(msg=nil)
      @etime = Time.now.utc.to_i
      self.kind.success!
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
      if to.to_s == current_queue.to_s
        raise Bluth::Buster, "Cannot move job to the queue it's in: #{to}"
      end
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

