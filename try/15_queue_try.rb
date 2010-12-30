require 'bluth'
require 'bluth/test_helpers'

Bluth::Queue.critical.clear

## Knows queue names
Bluth::Queue.queues.collect(&:name)
#=> [:critical, :high, :low, :running, :successful, :failed, :orphaned]

## Knows queue keys
Bluth::Queue.queues.collect(&:rediskey)
#=> ["bluth:queue:critical", "bluth:queue:high", "bluth:queue:low", "bluth:queue:running", "bluth:queue:successful", "bluth:queue:failed", "bluth:queue:orphaned"]

## Knows a queue
ret = Bluth::Queue.critical
ret.class
#=> Familia::List

## Can push on to a queue
Bluth::Queue.critical.push 'job1'
Bluth::Queue.critical.push 'job2'
Bluth::Queue.critical.push 'job3'
Bluth::Queue.critical.size
#=> 3

## Can shift from a queue
job = Bluth::Queue.critical.shift
#=> 'job1'

Bluth::Queue.critical.clear