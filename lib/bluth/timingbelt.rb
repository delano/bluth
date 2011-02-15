

module Bluth 
  
  module TimingBelt
    include Familia
    prefix [:bluth, :timingbelt]
    class << self
      attr_reader :notchcache
      def now mins=0, time=Time.now.utc
        time + (mins*60)
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
        @notchcache[key] ||= Familia::Set.new key, :ttl => 4*60*60 # 4 hours
        @notchcache[key]
      end
      def priority minutes=2, filter=nil, time=now
        (0..minutes).to_a.reverse.collect { |min| notch(min*-1, filter, time) }
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