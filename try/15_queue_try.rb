require 'bluth'
require 'bluth/test_helpers'

Bluth::Queue.critical.clear

## Knows queues
Bluth::Queue.queues
#=> [:critical, :high, :low, :running, :failed, :orphaned]

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