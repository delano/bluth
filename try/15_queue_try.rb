
require_relative '../lib/bluth'
require_relative '../lib/bluth/test_helpers'

Familia.debug = true
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


## Can create a queue on the fly
q = Bluth::Queue.create_queue :anything
q.rediskey
#=> "bluth:queue:anything"

## And that new queue has a method
q = Bluth::Queue.queue :anything
q.class
#=> Familia::List

## We can get a list of queues by priority
Bluth::Queue.entry_queues.collect { |q| q.name }
#=> [:critical, :high, :low]


Bluth::Queue.critical.clear
