# frozen_string_literal: false

require 'httparty'
require 'aws-sdk'
require 'json'
require 'logger'

#log = Logger.new(STDOUT)
#error_log = Logger.new(STDERR)

def adobe_sign_scraper

	new_access_token = get_new_access_token

	all_agreements = get_all_agreements(new_access_token: new_access_token)

	all_agreements_with_only_required_fields = extract_only_required_fields_from_all_agreements(all_agreements: all_agreements)
	
	agreements_loaded_into_dynamodb = load_all_agreements_into_dynamodb(all_agreements_with_only_required_fields: all_agreements_with_only_required_fields)
end

def get_new_access_token

	get_new_access_token_url = 'https://api.eu1.echosign.com/oauth/refresh'

	get_new_access_token_headers = { 
		"Content-Type" 	=> "application/json"
	}

	get_new_access_token_body = {
		:refresh_token	=> "XXXX",
		:client_id		=> "XXXX",
		:client_secret	=> "XXXX",
		:grant_type 	=> "refresh_token"
	}

	begin
		get_new_access_token_response = HTTParty.post(
			get_new_access_token_url,
			:body => get_new_access_token_body
		)

		data = JSON.parse(get_new_access_token_response.body)
		new_access_token = data['access_token']
		return new_access_token

	rescue Exception => e
  		puts "Error occurred: #{e.message}"
  		puts "#{e.backtrace}"
	end
end

def get_all_agreements(new_access_token:)
	all_agreements_array = []
	required_fields_array = ['id', 'name', 'displayDate', 'displayParticipantSetInfos', 'status']

	get_all_agreements_url = 'https://api.eu1.echosign.com/api/rest/v6/agreements?pageSize=50'

	get_all_agreements_headers = { 
		"Accept" 		=> "application/json",
		"Content-Type" 	=> "application/json",
		"Authorization" => "Bearer #{new_access_token}"
	}

	begin
		get_all_agreements_response = HTTParty.get(
			get_all_agreements_url,
			:headers => get_all_agreements_headers
		)

		data = JSON.parse(get_all_agreements_response.body)
		data['userAgreementList'].each do |single_agreement|
			if single_agreement['name'] == "Test agreement"
				all_agreements_array.push(single_agreement)
			end
		end

		while data['page']['nextCursor'] do
			get_all_agreements_response = HTTParty.get(
				get_all_agreements_url.to_s + '&cursor=' + data['page']['nextCursor'],
				:headers => get_all_agreements_headers
				)

			data = JSON.parse(get_all_agreements_response.body)
			data['userAgreementList'].each do |single_agreement|
				if single_agreement['name'] == "Test agreement"
					all_agreements_array.push(single_agreement)
				end
			end
		end

	rescue Exception => e
  		puts "Error occurred: #{e.message}"
  		puts "#{e.backtrace}"
	end
	#puts JSON.generate(all_agreements_array)
	return all_agreements_array
end

def extract_only_required_fields_from_all_agreements(all_agreements:)
	all_agreements_with_only_required_fields_array = []
	required_fields_array = ['id', 'name', 'displayDate', 'displayParticipantSetInfos', 'status']
	all_agreements.each do |single_agreement|
		agreement_hash_containing_arrays = single_agreement.select {|key, value| required_fields_array.include?(key) }
		agreement_hash = Hash.new
		agreement_hash_containing_arrays.each do |key, value|
			if (value.instance_of? Array)
				if key == "displayParticipantSetInfos"
					value.each do |recipients|
						recipients['displayUserSetMemberInfos'].each do |key2, value2|
							if key2['fullName']
								agreement_hash[('recipient_full_name_' + (value.index(recipients).to_i + 1).to_s)] = key2['fullName']
								agreement_hash[('recipient_email_' + (value.index(recipients).to_i + 1).to_s)] = key2['email']
							elsif key2['fullName'].nil?
								agreement_hash[('recipient_full_name_' + (value.index(recipients).to_i + 1).to_s)] = "NoName"
								agreement_hash[('recipient_email_' + (value.index(recipients).to_i + 1).to_s)] = key2['email']
							end
						end
					end
				end
			
			elsif key == 'id'
				agreement_hash['agreement_id'] = value
			elsif key == 'name'
				agreement_hash['agreement_name'] = value
			elsif key == 'displayDate' # or 'createdDate'
				agreement_hash['agreement_date'] = value
			elsif key == 'status'
				agreement_hash['agreement_status'] = value
			end
		end
		all_agreements_with_only_required_fields_array.push(agreement_hash)
	end
	#puts all_agreements_with_only_required_fields_array
	return all_agreements_with_only_required_fields_array
end

def load_all_agreements_into_dynamodb(all_agreements_with_only_required_fields:)
	dynamodb = Aws::DynamoDB::Client.new
	table_name = 'adobe_sign_agreements_status'
	all_agreements_with_only_required_fields.each do |agreement|

		item = {
			agreement_id: agreement['agreement_id'],
			agreement_status: agreement['agreement_status'],
			agreement_name: agreement['agreement_name'],
			agreement_date: agreement['agreement_date'],
			recipient_full_name: agreement['recipient_full_name_1'], # only 1 recipient needed
			recipient_email: agreement['recipient_email_1'] # only 1 recipient needed
		}

		params = {
		    table_name: table_name,
		    item: item
		}

		begin
		    dynamodb.put_item(params)
		    puts "Successfully added agreement for: #{agreement['recipient_email_1']}"

		rescue  Aws::DynamoDB::Errors::ServiceError => e
		    puts "Unable to add agreement for: #{agreement['recipient_email_1']}"
		    puts "#{e.message}"
		end
	end
end

adobe_sign_scraper
