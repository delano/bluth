require 'bluth'
require 'bluth/timingbelt'
require 'bluth/test_helpers'

@now = Time.at(1297732507).utc # 2011-02-14 20:15:07 -0500
Bluth::TimingBelt.redis.flushdb

## Knows now
Bluth::TimingBelt.now(0, @now).to_s
#=> '2011-02-15 01:15:07 UTC'

## Now can have an offset
Bluth::TimingBelt.now(5, @now).to_s
#=> '2011-02-15 01:20:07 UTC'

## Can create a timestamp
Bluth::TimingBelt.stamp 0, @now
#=> '01:15'

## Knows the current key
Bluth::TimingBelt.rediskey 0, nil, @now
#=> 'bluth:timingbelt:01:15'

## Creates a Set object for the current time
Bluth::TimingBelt.notch(0, nil, @now).class
#=> Familia::Set

## Set for the current time doesn't exist
Bluth::TimingBelt.notch(0, nil, @now).exists?
#=> false

## Set for the current time is empty
Bluth::TimingBelt.notch(0, nil, @now).empty?
#=> true

## Knows the current set priority
Bluth::TimingBelt.priority(2, nil, @now).collect { |q| q.name }
#=> ["bluth:timingbelt:01:13", "bluth:timingbelt:01:14", "bluth:timingbelt:01:15"]

## Handler can ennotch right now
ExampleHandler.ennotch({}, 0, nil, @now).notch
#=> 'bluth:timingbelt:01:15'

## Handler can ennotch 1 minute ago
ExampleHandler.ennotch({}, -1, nil, @now).notch
#=> 'bluth:timingbelt:01:14'

## Will get a gob from the highest priority notch
gob = Bluth::TimingBelt.pop(2, nil, @now)
gob.notch
#=> 'bluth:timingbelt:01:14'

## Will get a gob from the next priority notch
gob = Bluth::TimingBelt.pop(2, nil, @now)
gob.notch
#=> 'bluth:timingbelt:01:15'

