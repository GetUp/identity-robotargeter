module IdentityRobotargeter
  class Redirect < ApplicationRecord
    include ReadOnly
    self.table_name = "redirects"
    belongs_to :callee
    belongs_to :campaign

    scope :updated_redirects, -> (last_updated_at) {
      includes(:callee, :campaign)
      .where('redirects.created_at >= ?', last_updated_at)
      .order('redirects.created_at')
      .limit(Settings.robotargeter.pull_batch_amount)
    }

    scope :updated_redirects_all, -> (last_updated_at) {
      includes(:callee, :campaign)
      .where('redirects.created_at >= ?', last_updated_at)
    }
  end
end
