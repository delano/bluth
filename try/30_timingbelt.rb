require 'bluth'
require 'bluth/timingbelt'
require 'bluth/test_helpers'

Familia.debug = true

@now = Time.at(1297641600).utc # 2011-02-14 20:00:00
Bluth::TimingBelt.redis.flushdb

## Knows now
Bluth::TimingBelt.now(0, @now).to_s
#=> '2011-02-14 00:00:00 UTC'

## Now can have an offset
Bluth::TimingBelt.now(5, @now).to_s
#=> '2011-02-14 00:05:00 UTC'

## Can create a timestamp
Bluth::TimingBelt.stamp 0, @now
#=> '00:00'

## Knows the current key
Bluth::TimingBelt.rediskey 0, nil, @now
#=> 'bluth:timingbelt:00:00'

## Creates a Set object for the current time
Bluth::TimingBelt.notch(0, nil, @now).class
#=> Familia::Set

## A notch knows its stamp
Bluth::TimingBelt.notch(0, nil, @now).stamp
#=> '00:00'

## A notch knows the next stamp
Bluth::TimingBelt.notch(0, nil, @now).next.stamp
#=> '00:01'

## A notch knows the previous stamp
Bluth::TimingBelt.notch(0, nil, @now).prev.stamp
#=> '23:59'

## Set for the current time doesn't exist
Bluth::TimingBelt.notch(0, nil, @now).exists?
#=> false

## Set for the current time is empty
Bluth::TimingBelt.notch(0, nil, @now).empty?
#=> true

## Knows the current set priority
Bluth::TimingBelt.priority(2, nil, @now).collect { |q| q.name }
#=> ["bluth:timingbelt:23:58", "bluth:timingbelt:23:59", "bluth:timingbelt:00:00"]

## Handler can engauge right now
ExampleHandler.engauge({}, 0, nil, @now).notch
#=> 'bluth:timingbelt:00:00'

## Handler can engauge 1 minute ago
ExampleHandler.engauge({}, -1, nil, @now).notch
#=> 'bluth:timingbelt:23:59'

## Handler can engauge 10 minutes from now
@gob3 = ExampleHandler.engauge({}, 10, nil, @now)
@gob3.notch
#=> 'bluth:timingbelt:00:10'

## Will get a job from the highest priority notch
@gob1 = Bluth::TimingBelt.pop(2, nil, @now)
@gob1.notch
#=> 'bluth:timingbelt:23:59'

## Will get a job from the next priority notch
@gob2 = Bluth::TimingBelt.pop(2, nil, @now)
@gob2.notch
#=> 'bluth:timingbelt:00:00'

## Knows next available notch
@next_notch = Bluth::TimingBelt.next_empty_notch(nil, @now)
@next_notch.name unless @next_notch.nil?
#=> 'bluth:timingbelt:00:01'

## Knows next available notch
notches = Bluth::TimingBelt.find(@gob3.jobid, nil, @now)
notches.first.name unless notches.first.nil?
#=> 'bluth:timingbelt:00:10'


