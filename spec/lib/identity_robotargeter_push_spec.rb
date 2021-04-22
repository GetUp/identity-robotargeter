require 'rails_helper'

describe IdentityRobotargeter do
  before(:each) do
    clean_external_database

    @sync_id = 1
    @robotargeter_campaign = FactoryBot.create(:robotargeter_campaign)
    @external_system_params = JSON.generate({'campaign_id' => @robotargeter_campaign.id, 'priority': '2', 'phone_type': 'mobile'})

    2.times { FactoryBot.create(:member_with_mobile) }
    FactoryBot.create(:member_with_landline)
    FactoryBot.create(:member)
  end

  context '#push' do
    before(:each) do
      @members = Member.all
    end

    context 'with valid parameters' do
      it 'has created an attributed Audience in Robotargeter' do
        IdentityRobotargeter.push(@sync_id, @members, @external_system_params) do |members_with_phone_numbers, campaign_name|
          @robotargeter_audience = IdentityRobotargeter::Audience.find_by_campaign_id(@robotargeter_campaign.id)
          expect(@robotargeter_audience).to have_attributes(campaign_id: @robotargeter_campaign.id, sync_id: 1, status: 'initialising', priority: 2)
        end
      end
      it 'yeilds correct campaign_name' do
        IdentityRobotargeter.push(@sync_id, @members, @external_system_params) do |members_with_phone_numbers, campaign_name|
          expect(campaign_name).to eq(@robotargeter_campaign.name)
        end
      end
      it 'yeilds members_with_phone_numbers' do
        IdentityRobotargeter.push(@sync_id, @members, @external_system_params) do |members_with_phone_numbers, campaign_name|
          expect(members_with_phone_numbers.count).to eq(2)
        end
      end
    end

    context 'with invalid priority parameters' do
      it 'has created an attributed Audience in Robotargeter' do
        invalid_external_system_params = JSON.generate({'campaign_id' => @robotargeter_campaign.id, 'priority': 'yada yada', 'phone_type': 'mobile'})
        IdentityRobotargeter.push(@sync_id, @members, invalid_external_system_params) do |members_with_phone_numbers, campaign_name|
          @robotargeter_audience = IdentityRobotargeter::Audience.find_by_campaign_id(@robotargeter_campaign.id)
          expect(@robotargeter_audience).to have_attributes(campaign_id: @robotargeter_campaign.id, sync_id: 1, status: 'initialising', priority: 1)
        end
      end
    end
  end

  context '#push_in_batches' do
    before(:each) do
      @members = Member.all.with_phone_type('mobile')
      @audience = FactoryBot.create(:robotargeter_audience, sync_id: @sync_id, campaign_id: @robotargeter_campaign.id, priority: 2)
    end

    context 'with valid parameters' do
      it 'updates attributed Audience in Robotargeter' do
        IdentityRobotargeter.push_in_batches(1, @members, @external_system_params) do |batch_index, write_result_count|
          audience = IdentityRobotargeter::Audience.find_by_campaign_id(@robotargeter_campaign.id)
          expect(audience).to have_attributes(status: 'active')
        end
      end
      it 'yeilds correct batch_index' do
        IdentityRobotargeter.push_in_batches(1, @members, @external_system_params) do |batch_index, write_result_count|
          expect(batch_index).to eq(0)
        end
      end
      it 'yeilds write_result_count' do
        IdentityRobotargeter.push_in_batches(1, @members, @external_system_params) do |batch_index, write_result_count|
          expect(write_result_count).to eq(2)
        end
      end
    end
  end
end
