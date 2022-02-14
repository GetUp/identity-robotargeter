$:.push File.expand_path("../lib", __FILE__)

# Maintain your gem's version:
require "identity_robotargeter/version"

# Describe your gem and declare its dependencies:
Gem::Specification.new do |s|
  s.name        = "identity_robotargeter"
  s.version     = IdentityRobotargeter::VERSION
  s.authors     = ["GetUp!"]
  s.email       = ["tech@getup.org.au"]
  s.homepage    = "https://github.com/GetUp/identity_robotargeter"
  s.summary     = "Identity Robotargeter Integration."
  s.description = "Push members to Robotargeter Audience. Pull Responses to Contact Campaigns."
  s.license     = "TBD"

  s.files = Dir["{app,config,db,lib}/**/*", "Rakefile", "README.md"]

  s.add_dependency "rails"
  s.add_dependency "pg"
  s.add_dependency "active_model_serializers"

end
