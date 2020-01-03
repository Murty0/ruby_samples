# frozen_string_literal: false

require 'aws-sdk-s3'
require 'date'
require 'time'
require 'json'
require 'csv'
require 'prawn'
require 'prawn/table'

def log_created_within_dates?(extracted_date:, start_date:, end_date:)
  (extracted_date >= start_date &&
    extracted_date <= end_date)
end

def bucket_objects(client:, bucket_name:)
  bucket_objects = []
  client.list_objects(bucket: bucket_name, prefix: 'logs.').each do |response|
    response.contents.each do |obj|
      bucket_objects << obj
    end
  end
  bucket_objects
end

def extract_date_from_object_key(obj:)
  time_string = obj.key.match(/\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+/).to_s # rubocop:disable Metrics/LineLength
  return Time.parse(time_string) unless time_string.nil?

  puts "Unable to find timestamp in #{obj.key}"
end

def keys_for_all_objects_within_dates(objects:, start_date:, end_date:) # rubocop:disable Metrics/LineLength, Metrics/MethodLength
  objects_keys = []
  objects.each do |obj|
    extracted_date = extract_date_from_object_key(
      obj: obj
    )

    next if extracted_date.nil?

    next unless log_created_within_dates?(
      extracted_date: extracted_date,
      start_date: start_date,
      end_date: end_date
    )

    objects_keys << obj.key
  end
  objects_keys
end

def query_logs_within_dates_with_filters(client:, bucket_objects_keys:) # rubocop:disable Metrics/LineLength, Metrics/MethodLength
  filtered_events = []
  bucket_objects_keys.each do |key|
    params = {
      bucket: 'logs-archive',
      key: key,
      expression_type: 'SQL',
      expression: "SELECT s.eventType, s.actor.displayName, s.actor.alternateId,
        s.actor.type, s.target[0].displayName, s.target[1].displayName,
        s.target[2].displayName, s.outcome.result, s.published
        from S3Object[*][*] s
        WHERE s.eventType IN ('user.lifecycle.create',
        'user.lifecycle.activate', 'user.lifecycle.deactivate',
        'application.user_membership.add', 'application.user_membership.remove',
        'group.user_membership.add', 'group.user_membership.remove')",
      input_serialization: {
        compression_type: 'GZIP',
        json: { type: 'LINES' }
      },
      output_serialization: { csv: {} }
    }

    client.select_object_content(params) do |stream|
      stream.on_records_event do |event|
        filtered_events << event.payload.read
      end
    end
  end
  filtered_events
end

def filtered_logs_hashed_with_new_unique_keys(filtered_logs:) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/LineLength, Metrics/MethodLength, Metrics/PerceivedComplexity
  events_hash_array = []
  filtered_logs.each do |filtered_log| # rubocop:disable Metrics/BlockLength
    begin
    CSV.parse(filtered_log) do |row| # rubocop:disable Metrics/BlockLength
      actor_type = (row[3] == 'SystemPrincipal' ? 'System' : row[3])
      event_type = row[0]
      case event_type
      when 'user.lifecycle.create'
        event_type = 'User Created'
      when 'user.lifecycle.activate'
        event_type = 'User Activated'
      when 'user.lifecycle.deactivate'
        event_type = 'User Deactivated'
      when 'application.user_membership.add'
        event_type = 'Application Membership Added'
      when 'application.user_membership.remove'
        event_type = 'Application Membership Removed'
      when 'group.user_membership.add'
        event_type = 'Group Membership Added'
      when 'group.user_membership.remove'
        event_type = 'Group Membership Removed'
      else
        event_type
      end

      if row.length == 7 # rubocop: disable Style/ConditionalAssignment
        event_hash = { event_type: event_type,
                       actor_display_name: row[1],
                       actor_alternate_id: row[2],
                       actor_type: actor_type,
                       target_display_name1: row[4],
                       target_display_name2: 'not applicable',
                       target_display_name3: 'not applicable',
                       outcome_result: row[5],
                       published: row[6] }
      elsif row.length == 8
        event_hash = { event_type: event_type,
                       actor_display_name: row[1],
                       actor_alternate_id: row[2],
                       actor_type: actor_type,
                       target_display_name1: row[4],
                       target_display_name2: row[5],
                       target_display_name3: 'not applicable',
                       outcome_result: row[6],
                       published: row[7] }
      elsif row.length == 9
        event_hash = { event_type: event_type,
                       actor_display_name: row[1],
                       actor_alternate_id: row[2],
                       actor_type: actor_type,
                       target_display_name1: row[4],
                       target_display_name2: row[5],
                       target_display_name3: row[6],
                       outcome_result: row[7],
                       published: row[8] }
      else
        event_hash = { event_type: event_type,
                       actor_display_name: row[1],
                       actor_alternate_id: row[2],
                       actor_type: actor_type,
                       target_display_name1: row[4],
                       target_display_name2: row[5],
                       target_display_name3: row[6],
                       outcome_result: row[7],
                       published: row[8] }
      end
      events_hash_array << event_hash
    end
    rescue CSV::MalformedCSVError => e
      puts e.message
  end
  end
  events_hash_array
end

def generate_pdf(hashed_logs:, file_headers:, start_date:, end_date:) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/LineLength, Metrics/MethodLength
  items = []
  begin
    Prawn::Document.generate( # rubocop:disable Metrics/BlockLength
      "Oktalogs-#{start_date}-to-#{end_date}.pdf",
      page_layout: :landscape,
      margin: [50, 20, 50, 20]
    ) do
      font_families.update('Roboto' => {
                             normal: "#{Dir.pwd}/Roboto-Regular.ttf",
                             bold: "#{Dir.pwd}/Roboto-Bold.ttf"
                           })
      font('Roboto') do # rubocop:disable Metrics/BlockLength
        define_grid(columns: 5, rows: 20, gutter: 10)

        grid([0, 0], [1, 1]).bounding_box do
          text 'Okta Report', size: 14, align: :left, color: '57bd9d', style: :bold # rubocop:disable Metrics/LineLength
          text 'Report Type: Activity per date', size: 8, align: :left, color: '017dbb' # rubocop:disable Metrics/LineLength
          text "Report Date: #{start_date} to #{end_date}", size: 8, align: :left, color: '017dbb' # rubocop:disable Metrics/LineLength
          move_down 1
        end

        items = [file_headers]
        items += hashed_logs.each.map do |item|
          [
            item[:event_type],
            (item[:actor_display_name].nil? ? item[:actor_display_name] : item[:actor_display_name].force_encoding('ISO-8859-1')), # rubocop:disable Metrics/LineLength
            (item[:actor_alternate_id].nil? ? item[:actor_alternate_id] : item[:actor_alternate_id].force_encoding('ISO-8859-1')), # rubocop:disable Metrics/LineLength
            item[:actor_type],
            (item[:target_display_name1].nil? ? item[:target_display_name1] : item[:target_display_name1].force_encoding('ISO-8859-1')), # rubocop:disable Metrics/LineLength
            (item[:target_display_name2].nil? ? item[:target_display_name2] : item[:target_display_name2].force_encoding('ISO-8859-1')), # rubocop:disable Metrics/LineLength
            (item[:target_display_name3].nil? ? item[:target_display_name3] : item[:target_display_name3].force_encoding('ISO-8859-1')), # rubocop:disable Metrics/LineLength
            item[:outcome_result],
            item[:published]
          ]
        end

        table(
          items,
          header: true,
          row_colors: ['f5f5f5'],
          column_widths: [115, 85, 100,
                          50, 85, 85,
                          85, 45, 100], # <= 572 in portrait
          cell_style: {
            border_color: 'ffffff',
            border_width: 1,
            padding: [2, 1, 0, 3], # top, right, bottom, left
            overflow: :shrink_to_fit,
            height: 20,
            size: 7,
            min_font_size: 7,
            text_color: '000080'
          }
        ) do
          row(0).font_style = :bold
          row(0).font_size = 8
          row(0).text_color = 'f5f5f5'
          row(0).background_color = '000080'
        end

        number_pages '<page>',
                     start_count_at: 1,
                     at: [bounds.right - 50, 0],
                     align: :right,
                     size: 9
      end
    end
    puts "PDF generated for the following dates: #{start_date} to #{end_date}"
  rescue Prawn::Errors::IncompatibleStringEncoding => e
    puts e.message
  end
end

def generate_csv(hashed_logs:, file_headers:, start_date:, end_date:) # rubocop:disable Metrics/LineLength, Metrics/MethodLength
  CSV.open(
    "Oktalogs-#{start_date}-to-#{end_date}.csv",
    'w',
    write_headers: true,
    headers: file_headers
  ) do |csv|
    hashed_logs.each do |h|
      csv << h.values
    end
  end
  puts "CSV generated for the following dates: #{start_date} to #{end_date}"
end

if $PROGRAM_NAME == __FILE__

  # assume-role to correct AWS role

  print 'Enter start date (in the format YYYY-MM-DD): '
  start_date = Time.parse(gets.chomp)
  print 'Enter end date (in the format YYYY-MM-DD): '
  end_date = Time.parse(gets.chomp)
  puts 'Generating report...'

  client = Aws::S3::Client.new(region: 'us-east-1')

  bucket_name = 'logs-archive'
  file_headers = %w[Event Actor ActorEmail ActorType
                    Target1 Target2 Target3 Outcome Published]

  log_start_date = Time.parse(start_date.to_s)
  log_end_date = Time.parse(end_date.to_s)

  bucket_objects_array = bucket_objects(
    client: client,
    bucket_name: bucket_name
  )

  keys_array = keys_for_all_objects_within_dates(
    objects: bucket_objects_array,
    start_date: log_start_date,
    end_date: log_end_date
  )

  filtered_logs_array = query_logs_within_dates_with_filters(
    client: client,
    bucket_objects_keys: keys_array
  )

  hashed_logs_array = filtered_logs_hashed_with_new_unique_keys(
    filtered_logs: filtered_logs_array
  )

  generate_pdf(
    hashed_logs: hashed_logs_array,
    file_headers: file_headers,
    start_date: start_date,
    end_date: end_date
  )

  generate_csv(
    hashed_logs: hashed_logs_array,
    file_headers: file_headers,
    start_date: start_date,
    end_date: end_date
  )
end
