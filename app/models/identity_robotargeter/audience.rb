module IdentityRobotargeter
  class Audience < ReadWrite
    self.table_name = "audiences"
    belongs_to :campaign
  end
end
