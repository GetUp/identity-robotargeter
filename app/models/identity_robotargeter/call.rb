module IdentityRobotargeter
  class Call < ReadOnly
    self.table_name = "calls"
    belongs_to :callee
    has_many :survey_results
    delegate :campaign, to: :callee, allow_nil: true

    scope :updated_calls, -> (last_updated_at, last_id) {
      includes({ callee: [:campaign] }, :survey_results)
      .references(:campaign)
      .where('campaigns.sync_to_identity')
      .where('calls.outgoing AND calls.callee_id is not null')
      .where('calls.updated_at > ? or (calls.updated_at = ? and calls.id > ?)', last_updated_at, last_updated_at, last_id)
      .order('calls.updated_at, calls.id')
      .limit(Settings.robotargeter.pull_batch_amount)
    }

    scope :updated_calls_all, -> (last_updated_at, last_id) {
      includes({ callee: [:campaign] }, :survey_results)
      .references(:campaign)
      .where('campaigns.sync_to_identity')
      .where('calls.outgoing AND calls.callee_id is not null')
      .where('calls.updated_at > ? or (calls.updated_at = ? and calls.id > ?)', last_updated_at, last_updated_at, last_id)
    }
  end
end
