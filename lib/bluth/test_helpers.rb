

class ExampleJob < Bluth::Gob
  #TODO queue Bluth::Critical
  
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
