module IdentityRobotargeter
  class Redirect < ReadOnly
    self.table_name = "redirects"
    belongs_to :callee
    belongs_to :campaign

    scope :updated_redirects, -> (last_updated_at, last_id, exclude_from) {
      includes(:callee, :campaign)
      .where('redirects.created_at > ? or (created_at = ? and id > ?)', last_updated_at, last_updated_at, last_id)
        .and(where('created_at < ?', exclude_from))
      .order('redirects.created_at')
      .limit(Settings.robotargeter.pull_batch_amount)
    }

    scope :updated_redirects_all, -> (last_updated_at, last_id, exclude_from) {
      where('redirects.created_at > ? or (created_at = ? and id > ?)', last_updated_at, last_updated_at, last_id)
        .and(where('created_at < ?', exclude_from))
    }
  end
end
