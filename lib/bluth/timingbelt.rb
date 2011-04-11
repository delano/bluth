require 'time'

module Bluth 
  
  module TimingBelt 
    include Familia
    prefix [:bluth, :timingbelt]
    # This module extends the Familia::Set that represents 
    # a notch. IOW, these are instance methods for notch objs.
    module Notch
      attr_accessor :stamp, :filter, :time
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
      def queue
        Bluth::Queue.create_queue stamp
      end
      def -(other)
        ((self.time - other.time)/60).to_i
      end
    end
    @length = 60 # minutes
    class << self
      attr_reader :notchcache, :length
      def find v, mins=length, filter=nil, time=now
        raise ArgumentError, "value cannot be nil" if v.nil?
        select(mins, filter, time) do |notch| 
          notch.member?(v)
        end
      end
      def range rng, filter=nil, time=now, &blk
        rng.to_a.each { |idx|
          notch = Bluth::TimingBelt.notch idx, filter, time
          blk.call notch
        }
      end
      # mins: the number of minutes to look ahead. 
      def each mins=length, filter=nil, time=now, &blk
        mins.times { |idx|
          notch = Bluth::TimingBelt.notch idx, filter, time
          blk.call notch
        }
      end
      def select mins=length, filter=nil, time=now, &blk
        ret = []
        each(mins, filter, time) { |notch| ret << notch if blk.call(notch) }
        ret
      end
      def collect mins=length, filter=nil, time=now, &blk
        ret = []
        each(mins, filter, time) { |notch| ret << blk.call(notch) }
        ret
      end
      def now mins=0, time=Time.now.utc
        time + (mins*60)  # time wants it in seconds
      end
      def stamp mins=0, time=now
        (time + (mins*60)).strftime('%H:%M')
      end
      def notch mins=0, filter=nil, time=now
        cache_key = [now(mins, time).to_i, filter].join(':')
        key = rediskey(stamp(mins, time), filter)
        @notchcache ||= {}
        if @notchcache[cache_key].nil?
          @notchcache[cache_key] ||= Familia::Set.new key, 
              :ttl => 2*60*60, # 2 hours
              :extend => Bluth::TimingBelt::Notch, 
              :db => Bluth::TimingBelt.db
          @notchcache[cache_key].stamp = stamp(mins, time) 
          @notchcache[cache_key].filter = filter
          @notchcache[cache_key].time = now(mins, time)
        end
        @notchcache[cache_key]
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
      def add data, notch=nil
        notch ||= Bluth::TimingBelt.notch 1
        notch.add data
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