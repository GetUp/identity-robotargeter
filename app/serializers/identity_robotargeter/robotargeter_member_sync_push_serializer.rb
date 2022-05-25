module IdentityRobotargeter
  class RobotargeterMemberSyncPushSerializer < ActiveModel::Serializer
    attributes :external_id, :first_name, :mobile_number, :campaign_id, :audience_id, :data, :callable

    def external_id
      @object.id
    end

    def first_name
      @object.first_name ? @object.first_name : ''
    end

    def mobile_number
      if instance_options[:phone_type] == 'all'
        number = @object.phone_numbers.first
      else
        number = @object.phone_numbers.send(instance_options[:phone_type]).first
      end
      number.phone if number
    end

    def campaign_id
      instance_options[:campaign_id]
    end

    def audience_id
      instance_options[:audience_id]
    end

    def data
      data = @object.flattened_custom_fields
      data['address'] = @object.address
      data['postcode'] = @object.postcode
      data["areas"] = @object.areas.each_with_index.map{|area, index|
        {
          name: area.name,
          code: area.code,
          area_type: area.area_type,
          party: area.party,
          representative_name: area.representative_name
        }
      }
      data.to_json
    end

    def callable
      true
    end
  end
end
