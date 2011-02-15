require 'time'

module Bluth 
  
  module TimingBelt
    include Familia
    prefix [:bluth, :timingbelt]
    module Notch
      attr_accessor :stamp, :filter
      def next
        skip
      end
      def prev
        skip -1
      end
      def skip mins=1
        time = Time.parse(stamp || '')
        Bluth::TimingBelt.notch mins, filter, time
      end
    end
    @length = 60 # minutes
    class << self
      attr_reader :notchcache, :length
      def find v, filter=nil, time=now
        raise ArgumentError, "value cannot be nil" if v.nil?
        select(filter, time) do |notch| 
          notch.member?(v)
        end
      end
      def each filter=nil, time=now, &blk
        length.times { |idx|
          notch = Bluth::TimingBelt.notch idx, filter, time
          blk.call notch
        }
      end
      def select filter=nil, time=now, &blk
        ret = []
        each(filter, time) { |notch| ret << notch if blk.call(notch) }
        ret
      end
      def collect filter=nil, time=now, &blk
        ret = []
        each(filter, time) { |notch| ret << blk.call(notch) }
        ret
      end
      def now mins=0, time=Time.now.utc
        time + (mins*60)  # time wants it in seconds
      end
      def stamp mins=0, time=now
        (time + (mins*60)).strftime('%H:%M')
      end
      def rediskey mins=0, filter=nil, time=now
        super stamp(mins, time), filter
      end
      def notch mins=0, filter=nil, time=now
        key = rediskey(mins, filter, time)
        @notchcache ||= {}
        if @notchcache[key].nil?
          @notchcache[key] ||= Familia::Set.new key, 
              :ttl => 4*60*60, # 4 hours
              :extend => Bluth::TimingBelt::Notch
          @notchcache[key].stamp = stamp(mins, time) 
          @notchcache[key].filter = filter
        end
        @notchcache[key]
      end
      def priority minutes=2, filter=nil, time=now
        (0..minutes).to_a.reverse.collect { |min| notch(min*-1, filter, time) }
      end
      def next_empty_notch filter=nil, time=now
        length.times { |min| 
          possible = notch min+1, filter, time  # add 1 so we don't start at 0
          return possible if possible.empty?
        }
        nil
      end
      def pop minutes=2, filter=nil, time=now
        gob = nil
        priority = Bluth::TimingBelt.priority minutes, filter, time
        begin
          gobid, notch = nil, nil
          priority.each { |n| gobid, notch = n.pop, n.name; break unless gobid.nil? }
          unless gobid.nil?
            Familia.ld "FOUND #{gobid} id #{notch}" if Familia.debug?
            gob = Bluth::Gob.from_redis gobid
            raise Bluth::Buster, "No such gob object: #{gobid}" if gob.nil?
            Bluth::Queue.running << gob.jobid
            gob.current_queue = :running
            gob.save
          end
        rescue => ex
          Familia.info ex.message
          Familia.ld ex.backtrace if Familia.debug?
        end
        gob
      end
    end
    
  end
end