require 'drydock'

module Bluth
  class CLI < Drydock::Command
    
    
    def start_workers
      start_worker
    end
    
    def start_scheduler
      start_worker Bluth.scheduler
    end
    
    def stop_scheduler
      stop_workers Bluth.scheduler
    end
    
    def schedulers
      workers Bluth.scheduler
    end
    
    #def flush_workers
    #  if @global.auto || Annoy.are_you_sure?
    #    Bluth::Worker.prefix
    #  end
    #end
    
    def start_worker worker_class=Bluth::Worker
      if @global.daemon
        worker = worker_class.new
        Familia.info "Created: #{worker.rediskey}"
        worker.daemonize
        worker.run
      else
        Bluth.queuetimeout = 3.seconds
        worker_class.run
      end
    end
    
    def stop_workers worker_class=Bluth::Worker
      Bluth.connect
      worker_class.instances.each do |worker|
        kill_worker worker, worker_class
      end
    end
    
    def stop_worker wid=nil, worker_class=Bluth::Worker
      Bluth.connect
      wids = wid ? [wid] : @argv
      wids.each do |wid|
        worker = worker_class.from_redis wid
        kill_worker worker, worker_class
      end
    end
    
    def replace_worker worker_class=Bluth::Worker
      Bluth.connect
      @global.daemon = true
      worker = worker_class.instances.first  # grabs the oldest worker
      kill_worker worker, worker_class
      start_worker worker_class
    end
    
    def workers worker_class=Bluth::Worker
      Familia.info worker_class.all.collect &:rediskey
    end
    
    private 
    
    def kill_worker worker, worker_class=Bluth::Worker
      if worker.nil?
        Familia.info "No such worker"
        exit 1
      else
        Familia.info "Killing #{worker.rediskey}"
        worker.kill @option.force
      end
    end
    
  end
end
