# frozen_string_literal: false

require 'httparty'
require 'aws-sdk'
require 'json'
require 'logger'

#log = Logger.new(STDOUT)
#error_log = Logger.new(STDERR)

def dynamdodb_scanner

	all_unsigned_agreements_from_dynamodb = get_all_unsigned_agreements_from_dynamodb
	
end

def get_all_unsigned_agreements_from_dynamodb
	all_unsigned_agreements_array = []
	dynamodb = Aws::DynamoDB::Client.new
	table_name = "adobe_sign_agreements_status"

	params = {
	    table_name: table_name,
	    filter_expression: "NOT agreement_status IN (:status)",
	    expression_attribute_values: {
	    	":status" => "SIGNED"
	    },
	    limit: 1 #change
	}

	puts "Querying table: #{table_name}";

	begin
	    loop do
		    result = dynamodb.scan(params)
		    puts "Query succeeded."
		    result.items.each{|agreement|
		         all_unsigned_agreements_array.push(agreement)
		    }

		    break if result.last_evaluated_key.nil?

	        puts "Scanning for more agreements..."
	        params[:exclusive_start_key] = result.last_evaluated_key
	    end

	 	return all_unsigned_agreements_array

	rescue  Aws::DynamoDB::Errors::ServiceError => error
	    puts "Unable to query table: #{table_name}"
	    puts "#{error.message}"
	end
end


dynamdodb_scanner
