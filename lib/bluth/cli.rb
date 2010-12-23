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
    
    def start_worker worker_class=Bluth::Worker
      if @global.daemon
        worker = worker_class.new
        Familia.info "Created: #{worker.rediskey}"
        worker.daemonize
        worker.run
      else
        Bluth.poptimeout = 3.seconds
        worker_class.run
      end
    end
    
    def stop_workers worker_class=Bluth::Worker
      worker_class.instances.each do |wid|
        kill_worker wid, worker_class
      end
    end
    
    def stop_worker wid=nil,worker_class=Bluth::Worker
      wids = wid ? [wid] : @argv
      wids.each do |wid|
        kill_worker wid, worker_class
      end
    end
    
    def workers worker_class=Bluth::Worker
      Familia.info worker_class.all.collect &:key
    end
    
    private 
    
    def kill_worker wid, worker_class=Bluth::Worker
      worker = worker_class.from_redis wid
      if worker.nil?
        Familia.info "No such worker"
        exit 1
      else
        Familia.info "Killing #{worker.rediskey}"
        worker.kill @global.auto
      end
    end
    
  end
end
