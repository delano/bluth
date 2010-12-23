require 'bluth'
require 'bluth/test_helpers'

Familia.debug = true

## Can enqueue a job
@job = ExampleJob.enqueue :arg1 => :val1
@job.class
#=> Bluth::Gob

## ExampleGob knows it's on critical queue
@job.current_queue
#=> :critical

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

## Popped job is still critical
@popped_job.current_queue
#=> :critical

## Popped job has args
@popped_job.data['arg1']
#=> 'val1'

@job.destroy!