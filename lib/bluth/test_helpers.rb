

module ExampleHandler
  extend Bluth::Handler
  queue :critical
  
  # :hostid => String
  # :force => Boolean (update all monitors)
  def self.enqueue(opts={})
    super opts, opts.delete(:queue)
  end
  
  def self.perform(data={})
    begin
      
    rescue => ex
      
    end
  end  
end

class ExampleWorker < Bluth::Worker
  
end


Bluth::Worker.onstart do
  puts "onstart called"
end

Bluth::Worker.onexit do
  puts "onexit called"
end