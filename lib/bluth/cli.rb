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
        Familia.info "Created: #{worker.index}"
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
      workers = worker_class.instances.members.select { |w| w.host == Bluth.sysinfo.hostname }
      if workers.empty?
        Familia.info "No workers running on #{Bluth.sysinfo.hostname}"
      else
        workers.sort! { |a,b| a.created <=> b.created }
        oldest_worker = workers.first
        Familia.info "Replacing #{oldest_worker.index}/#{oldest_worker.pid} (running since #{Time.at(oldest_worker.created).utc})"
        kill_worker oldest_worker, worker_class
        @global.daemon = true
        start_worker worker_class
      end
    end
    
    def workers worker_class=Bluth::Worker
      Familia.info worker_class.all.collect &:index
    end
    
    private 
    
    def kill_worker worker, worker_class=Bluth::Worker
      if worker.nil?
        Familia.info "No such worker"
        exit 1
      else
        Familia.info "Killing #{worker.index}"
        worker.kill @option.force
      end
    end
    
  end
end
