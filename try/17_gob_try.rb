require 'bluth'
require 'bluth/test_helpers'

Familia.debug = true

## Can enqueue a job
@job = ExampleHandler.enqueue :arg1 => :val1
@job.class
#=> Bluth::Gob

## Job knows it's on critical queue
@job.current_queue
#=> :critical

## Job knows it's handler
@job.handler
#=> ExampleHandler

## Bluth::Critical has job id
Bluth::Queue.critical.range.member? @job.jobid
#=> true

## Can fetch a job from queue
@gobid = Bluth::Queue.critical.pop
#=> @job.jobid

## Create Gob
@popped_job = Bluth::Gob.from_redis @gobid
@popped_job.jobid
#=> @job.jobid

## Popped job has args
@popped_job.data['arg1']
#=> 'val1'

## Popped job is still critical
@popped_job.current_queue
#=> :critical

## Move job to another queue
@popped_job.running!
#=> true

## Popped job is still critical
@popped_job.current_queue
#=> :running


@job.destroy!