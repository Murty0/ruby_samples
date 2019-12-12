# frozen_string_literal: false

require 'httparty'
require 'uri'
require 'aws-sdk'
require 'json'
require 'logger'

#log = Logger.new(STDOUT)
#error_log = Logger.new(STDERR)

def namely_scraper

	number_of_profiles_pages = get_number_of_pages

	all_profiles = get_all_namely_profiles_with_only_required_fields(number_of_profiles_pages: number_of_profiles_pages)

	namely_data_loaded = load_namely_data_into_dynamodb(all_profiles: all_profiles)

end

def get_number_of_pages
	get_number_of_pages_url = 'https://xxxx.namely.com/api/v1/profiles.json'

	get_number_of_pages_headers = { 
		"Accept" 		=> "application/json",
		"Content-Type" 	=> "application/json",
		"Authorization" => "Bearer XXXX"
		}

	begin
		get_number_of_pages_response = HTTParty.get(
			get_number_of_pages_url,
			:headers => get_number_of_pages_headers
			)

		total_number_of_profiles = (JSON.parse(get_number_of_pages_response.body)['meta']['total_count']).to_i
		puts "Number of pages retrieved from Namely successfully!"
		puts "Total number of profiles: #{total_number_of_profiles}"
		max_results_per_page = 50 # as per Namely API docs
		total_number_of_pages = (total_number_of_profiles/50).ceil
		puts "Total number of pages: #{total_number_of_pages}"
		return total_number_of_pages

	rescue Exception => e
  		puts "Error occurred: #{e.message}"
	end
end

def get_all_namely_profiles_with_only_required_fields(number_of_profiles_pages:)
	all_profiles_array = []
	required_fields_array = ['id', 'email', 'personal_email', 'user_status', 'reports_to']
	number_of_profiles_pages_array = [*1..number_of_profiles_pages]
	number_of_profiles_pages_array.each do |page|
		get_all_namely_profiles_url = "https://xxxx.namely.com/api/v1/profiles.json?page=#{page}&per_page=50"
    get_all_namely_profiles_headers = {
      "Accept" 		=> "application/json",
      "Content-Type" 	=> "application/json",
      "Authorization" => "Bearer XXXX"
		}

		begin
			get_all_namely_profiles_response = HTTParty.get(
				get_all_namely_profiles_url,
				:headers => get_all_namely_profiles_headers
				)

			profiles_blob = JSON.parse(get_all_namely_profiles_response.body)['profiles']
      profiles_blob.each do |single_profile|
        profile_with_reports_to_hash = single_profile.select {|key, value| required_fields_array.include?(key) }
        profile_hash = Hash.new
        profile_with_reports_to_hash.each do |key, value|
            if (value.instance_of? Array)
              value.each do |key2, value2|
                if key2['email']
                  profile_hash['reports_to_email'] = key2['email']
                end
              end
            else
              profile_hash[key] = value
          end
        end
        all_profiles_array.push(profile_hash)
      end
			
			puts "Profiles retrieved from page #{page}"

		rescue Exception => e
			puts "Error occurred: #{e.message}"
		end
	end
  
	puts "All profiles retrieved from Namely successfully!"
	return all_profiles_array
end

def load_namely_data_into_dynamodb(all_profiles:)
	dynamodb = Aws::DynamoDB::Client.new
	table_name = 'namely-data'
	all_profiles.each do |profile|

		item = {
			Email: profile['email'],
			NamelyID: profile['id'],
			PersonalEmail: profile['personal_email'],
			ReportsToEmail: profile['reports_to_email'],
			UserStatus: profile['user_status']
		}

		params = {
		    table_name: table_name,
		    item: item
		}

		begin
		    dynamodb.put_item(params)
		    puts "Successfully added profile: #{profile['email']}"

		rescue  Aws::DynamoDB::Errors::ServiceError => e
		    puts "Unable to add profile: #{profile['email']}"
		    puts "#{e.message}"
		end
	end
end

namely_scraper
