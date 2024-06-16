Gem::Specification.new do |s|
  s.name        = "bluth"
  s.version     = "0.1.0"
  s.authors     = ["Delano Mandelbaum"]
  s.email       = "gems@solutious.com"
  s.summary     = "A Redis queuing system built on top of Familia (w/ daemons!)"
  s.description = "A Redis queuing system built on top of Familia"
  s.homepage    = "https://github.com/delano/bluth"
  s.license     = "MIT"

  s.files         = Dir["{lib,bin}/**/*", "LICENSE.txt", "README.rdoc"]
  s.executables   = s.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  s.require_paths = ["lib"]

  # Ensure you keep or adjust metadata like rubygems_version if necessary
  s.rubygems_version = "3.5.9"
  s.required_ruby_version = ">= 2.7.8"
end
