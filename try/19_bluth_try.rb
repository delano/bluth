require 'bluth'
require 'bluth/test_helpers'

#Familia.debug = true

ExampleHandler.enqueue :item => :val1
ExampleHandler.enqueue :item => :val2
ExampleHandler.enqueue :item => :val3


## Critical queue should have 3 items
Bluth::Queue.critical.size
#=> 3

## Can set queuetimeout
Bluth.queuetimeout = 2
#=> 2

## Bluth.shift returns first value
@job1 = Bluth.shift
@job1.data['item']
#=> 'val1'

## Bluth.pop returns last value
@job2 = Bluth.pop
@job2.data['item']
#=> 'val3'

## Bluth.pop returns remaining value
@job3 = Bluth.pop
@job3.data['item']
#=> 'val2'

## Bluth.pop returns nil after waiting for queuetimeout
Bluth.pop
#=> nil


Bluth::Queue.critical.clear
@job1.destroy! if @job1
@job2.destroy! if @job2
@job3.destroy! if @job3