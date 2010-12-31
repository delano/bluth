class Queue
  include Familia
  prefix :queue
  def self.rangeraw(count=100)
    gobids = Queue.redis.lrange(rediskey, 0, count-1) || []
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
    Queue.redis.lrem rediskey, 0, gobid
  end
  def self.inherited(obj)
    obj.prefix self.prefix
    obj.suffix obj.to_s.split('::').last.downcase.to_sym
    raise Buster.new("Duplicate queue: #{obj.suffix}") if Bluth.queue?(obj.suffix)
    Bluth.queues[obj.suffix] = obj
    super(obj)
  end
  def self.rediskey(pref=nil,suff=nil)
    Familia.rediskey(pref || prefix, suff || suffix)
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
      Queue.redis.llen rediskey
    rescue => ex
      Familia.info ex.message, ex.backtrace
      0
    end
  end 
  def self.push(gobid)
    Queue.redis.lpush self.rediskey, gobid
  end
  
  def self.pop
    gobid = Queue.redis.rpoplpush rediskey, Bluth::Running.rediskey
    return if gobid.nil?
    Familia.trace "FOUND gob #{gobid} from #{self.rediskey}"
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
