require 'rails_helper'

describe IdentityRobotargeter do
  context '#pull' do
    before(:each) do
      clean_external_database
      @sync_id = 1
      @external_system_params = JSON.generate({'pull_job' => 'fetch_new_calls'})
    end

    context 'with valid parameters' do
      it 'should call the corresponding method'  do
        expect(IdentityRobotargeter).to receive(:fetch_new_calls).exactly(1).times.with(1)
        IdentityRobotargeter.pull(@sync_id, @external_system_params)
      end
    end
  end

  context 'fetching new calls' do

    before(:all) do
      Sidekiq::Testing.inline!
    end

    after(:all) do
      Sidekiq::Testing.fake!
    end

    before(:each) do
      clean_external_database
      $redis.reset

      @sync_id = 1
      @subscription = Subscription.create!(name: 'Robotargeting')
      Settings.stub_chain(:robotargeter, :subscription_id) { @subscription.id }
      Settings.stub_chain(:robotargeter, :push_batch_amount) { nil }
      Settings.stub_chain(:robotargeter, :pull_batch_amount) { nil }

      @time = Time.now - 120.seconds
      @robotargeter_campaign = FactoryBot.create(:robotargeter_campaign, name: 'Test')

      3.times do |n|
        callee = FactoryBot.create(:robotargeter_callee, first_name: "Bob#{n}", mobile_number: "6142770040#{n}", campaign: @robotargeter_campaign)
        call = FactoryBot.create(:robotargeter_call, created_at: @time, id: n, callee: callee, duration: 60, status: 'success', outgoing: true)
        call.survey_results << FactoryBot.build(:robotargeter_survey_result, question: 'disposition', answer: 'no answer')
        call.survey_results << FactoryBot.build(:robotargeter_survey_result, question: 'voting_intention', answer: 'labor')
        call.survey_results << FactoryBot.build(:robotargeter_survey_result, question: 'favorite_party', answer: 'labor')
      end
    end

    it 'should fetch the new calls and insert them' do
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(Contact.count).to eq(3)
      member = Member.find_by_phone('61427700401')
      expect(member).to have_attributes(first_name: 'Bob1')
      expect(member.contacts_received.count).to eq(1)
      expect(member.contacts_made.count).to eq(0)
    end

    it 'should record all details' do
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(Contact.find_by_external_id('1')).to have_attributes(duration: 60, system: 'robotargeter', contact_type: 'call', status: 'success')
      expect(Contact.find_by_external_id('1').happened_at.utc.to_s).to eq(@time.utc.to_s)
    end

    it 'works with a home phone number set' do
      callee = FactoryBot.create(:robotargeter_callee, first_name: 'HomeBoy', home_number: '61727700400', campaign: @robotargeter_campaign)
      call = FactoryBot.create(:robotargeter_call, created_at: @time, id: '123', callee: callee, duration: 60, status: 'success', outgoing: true)
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(Contact.last).to have_attributes(external_id: '123', duration: 60, status: 'success')

      expect(Contact.last.happened_at.utc.to_s).to eq(@time.utc.to_s)
      expect(Contact.last.contactee.phone).to eq('61727700400')
    end

    it 'should opt out people that need it' do
      member = FactoryBot.create(:member, name: 'BobNo')
      member.update_phone_number('61427700409')
      member.subscribe_to(@subscription)

      expect(member.is_subscribed_to?(@subscription)).to eq(true)

      callee = FactoryBot.create(:robotargeter_callee, first_name: 'BobNo', mobile_number: '61427700409', campaign: @robotargeter_campaign, opted_out_at: Time.now)
      call = FactoryBot.create(:robotargeter_call, id: IdentityRobotargeter::Call.maximum(:id).to_i + 1, created_at: @time, callee: callee, duration: 60, status: 'success', outgoing: true)

      IdentityRobotargeter.fetch_new_calls(@sync_id) {}

      member.reload
      expect(member.is_subscribed_to?(@subscription)).to eq(false)
    end

    it 'should assign a campaign' do
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(ContactCampaign.count).to eq(1)
      expect(ContactCampaign.first.contacts.count).to eq(3)
      expect(ContactCampaign.first).to have_attributes(name: @robotargeter_campaign.name, external_id: @robotargeter_campaign.id, system: 'robotargeter', contact_type: 'call')
    end

    it 'should match members receiving calls' do
      member = FactoryBot.create(:member, first_name: 'Bob1')
      member.update_phone_number('61427700401')
      puts member.contacts_received
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(member.contacts_received.count).to eq(1)
      expect(member.contacts_made.count).to eq(0)
    end

    it 'should upsert calls' do
      member = FactoryBot.create(:member, first_name: 'Janis')
      member.update_phone_number('61427700401')
      FactoryBot.create(:contact, contactee: member, external_id: '2')
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(Contact.count).to eq(3)
      expect(member.contacts_received.count).to eq(1)
    end

    it 'should be idempotent' do
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      contact_hash = Contact.all.select('contactee_id, contactor_id, duration, system, contact_campaign_id').as_json
      cr_count = ContactResponse.all.count
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(Contact.all.select('contactee_id, contactor_id, duration, system, contact_campaign_id').as_json).to eq(contact_hash)
      expect(ContactResponse.all.count).to eq(cr_count)
    end

    it 'should update the last_updated_at' do
      old_updated_at = $redis.with { |r| r.get 'robotargeter:calls:last_updated_at' }
      sleep 2
      callee = FactoryBot.create(:robotargeter_callee, first_name: 'BobNo', mobile_number: '61427700408', campaign: @robotargeter_campaign)
      call = FactoryBot.create(:robotargeter_call, id: IdentityRobotargeter::Call.maximum(:id).to_i + 1, created_at: @time, callee: callee, duration: 60, status: 'success', outgoing: true)
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      new_updated_at = $redis.with { |r| r.get 'robotargeter:calls:last_updated_at' }

      expect(new_updated_at).not_to eq(old_updated_at)
    end

    it 'should correctly save Survey Results' do
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      contact_response = ContactCampaign.last.contact_response_keys.find_by(key: 'voting_intention').contact_responses.first
      expect(contact_response.value).to eq('labor')
      contact_response = ContactCampaign.last.contact_response_keys.find_by(key: 'favorite_party').contact_responses.first
      expect(contact_response.value).to eq('labor')
      expect(Contact.last.contact_responses.count).to eq(3)
    end

    it 'works if there is no name' do
      callee = FactoryBot.create(:robotargeter_callee, mobile_number: '61427700409', campaign: @robotargeter_campaign)
      call = FactoryBot.create(:robotargeter_call, id: IdentityRobotargeter::Call.maximum(:id).to_i + 1, created_at: @time, callee: callee, duration: 60, status: 'success', outgoing: true)

      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(Contact.last.contactee.phone).to eq('61427700409')
    end

    it "skips if callee phone can't be matched" do
      callee = FactoryBot.create(:robotargeter_callee, mobile_number: '6142709', campaign: @robotargeter_campaign)
      call = FactoryBot.create(:robotargeter_call, id: IdentityRobotargeter::Call.maximum(:id).to_i + 1, created_at: @time, callee: callee, duration: 60, status: 'success', outgoing: true)

      expect(Notify).to receive(:warning)
      IdentityRobotargeter.fetch_new_calls(@sync_id) {}

      expect(Contact.count).to eq(3)
    end

    context('with force=true passed as parameter') do
      before { IdentityRobotargeter::Call.update_all(updated_at: '1960-01-01 00:00:00') }

      it 'should ignore the last_updated_at and fetch all calls' do
        IdentityRobotargeter.fetch_new_calls(@sync_id, force: true) {}
        expect(Contact.count).to eq(3)
      end
    end

    it 'ignores outgoing calls' do
      callee = FactoryBot.create(:robotargeter_callee, mobile_number: '61427700409', campaign: @robotargeter_campaign)
      call = FactoryBot.create(:robotargeter_call, id: IdentityRobotargeter::Call.maximum(:id).to_i + 1, created_at: @time, callee: callee, duration: 60, status: 'success', outgoing: false)

      IdentityRobotargeter.fetch_new_calls(@sync_id) {}
      expect(Contact.count).to eq(3)
    end
  end

  context '#fetching_new_redirects' do

    before(:all) do
      Sidekiq::Testing.inline!
    end

    after(:all) do
      Sidekiq::Testing.fake!
    end

    before(:each) do
      clean_external_database
      $redis.reset
      @sync_id = 1
      @time = Time.now - 120.seconds
      @robotargeter_campaign = FactoryBot.create(:robotargeter_campaign)
      3.times do |n|
        callee = FactoryBot.create(:robotargeter_callee, first_name: "Bob#{n}", mobile_number: "6142770040#{n}", campaign: @robotargeter_campaign)
        redirect = FactoryBot.create(:robotargeter_redirect, callee: callee, campaign: @robotargeter_campaign, created_at: @time)
      end
    end
     it 'should match existing members' do
      member = FactoryBot.create(:member, first_name: 'Bob1')
      member.update_phone_number('61427700401')
      IdentityRobotargeter.fetch_new_redirects(@sync_id) {}
      expect(member.actions.count).to eq(1)
    end
     it 'creates one and only one new action' do
      IdentityRobotargeter.fetch_new_redirects(@sync_id) {}
      expect(Action.count).to eq(1)
      action = Action.find_by(external_id: @robotargeter_campaign.id, technical_type: 'robotargeter_redirect')
      expect(action).to have_attributes(name: @robotargeter_campaign.name, action_type: 'call')
      expect(action.members.count).to eq(3)
      expect(action.member_actions.first.created_at.utc.to_s).to eq(@time.utc.to_s)
    end
     it 'should upsert redirects' do
      IdentityRobotargeter.fetch_new_redirects(@sync_id) {}
      $redis.with { |r| r.set 'robotargeter:redirects:last_created_at', '1970-01-01 00:00:00' }
      IdentityRobotargeter.fetch_new_redirects(@sync_id) {}
      action = Action.find_by(external_id: @robotargeter_campaign.id, technical_type: 'robotargeter_redirect')
      expect(action.members.count).to eq(3)
    end
  end

  context '#fetch_active_campaigns' do

    before(:all) do
      Sidekiq::Testing.inline!
    end

    after(:all) do
      Sidekiq::Testing.fake!
    end

    before(:each) do
      clean_external_database
      @sync_id = 1
      2.times do
        FactoryBot.create(:robotargeter_campaign_with_redirect_questions, status: 'active', name: 'Test')
      end
      FactoryBot.create(:robotargeter_campaign_with_redirect_questions, status: 'paused')
      FactoryBot.create(:robotargeter_campaign_with_redirect_questions, status: 'inactive')
    end

    it 'should create contact_campaigns' do
      IdentityRobotargeter.fetch_active_campaigns(@sync_id) {}
      expect(ContactCampaign.count).to eq(2)
      ContactCampaign.all.each do |campaign|
        expect(campaign).to have_attributes(
          name: 'Test',
          system: IdentityRobotargeter::SYSTEM_NAME,
          contact_type: IdentityRobotargeter::CONTACT_TYPE
        )
      end
    end

    it 'should create contact_response_keys' do
      IdentityRobotargeter.fetch_active_campaigns(@sync_id) {}
      expect(ContactResponseKey.count).to eq(4)
      expect(ContactResponseKey.where(key: 'vote_preference').count).to eq(2)
      expect(ContactResponseKey.where(key: 'action').count).to eq(2)
    end
  end
end
