module IdentityRobotargeter
  class SurveyResult < ReadOnly
    self.table_name = "survey_results"
    belongs_to :call
  end
end