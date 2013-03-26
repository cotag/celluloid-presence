# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "celluloid-presence/version"

Gem::Specification.new do |s|
  s.name        = "celluloid-presence"
  s.version     = Celluloid::Presence::VERSION

  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Stephen von Takach"]
  s.email       = ["steve@cotag.me"]
  s.homepage    = "https://github.com/cotag/celluloid-presence"
  s.summary     = "Node presence using ZooKeeper for celluloid services"
  s.description = s.summary

  s.add_dependency "celluloid"
  s.add_development_dependency "rspec"

  s.files = Dir["{lib}/**/*"] + ["MIT-LICENSE", "Rakefile", "README.textile"]
  s.test_files = Dir["spec/**/*"]
  s.require_paths = ["lib"]
end
