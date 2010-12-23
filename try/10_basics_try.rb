require 'bluth'
require 'bluth/test_helpers'


## Bluth.queues
Bluth.queues.keys.collect(&:to_s).sort
#=> ["critical", "failed", "high", "low", "orphaned", "running", "scheduled", "successful"]

## Can enqueue a job
@job = ExampleJob.enqueue :arg1 => :val1
@job.class
#=> Bluth::Gob

## ExampleGob knows it's on critical queue
@job.current_queue
#=> Bluth::Critical

## Bluth::Critical has job id
Bluth::Critical.range.collect(&:id).member? @job.id
#=> true

## Can fetch a job from queue
@popped_job = Bluth::Critical.pop
@popped_job.id 
#=> @job.id

## Popped job is set to running
@popped_job.current_queue
#=> Bluth::Running

## Popped job has args
@popped_job.data['arg1']
#=> 'val1'

@job.destroy!