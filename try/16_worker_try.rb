require_relative '../lib/bluth'
require_relative '../lib/bluth/test_helpers'

Familia.debug = true
Bluth::Queue.critical.clear


## Can create a Worker
@worker = Bluth::Worker.new 'host', 'user', 'wid'
@worker.index
#=> "host:user:wid"

## Worker has a redis key
@worker.rediskey
#=> 'bluth:worker:host:user:wid:object'

## Worker counts success
@worker.success!
@worker.success!
@worker.success.to_i
#=> 2


if @worker
  @worker.destroy!
end
