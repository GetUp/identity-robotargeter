module IdentityRobotargeter
  class Caller < ReadOnly
    self.table_name = "callers"
    has_many :calls
  end
end