require 'octokit'
require 'time'

def issue_created_or_updated_within_dates?(issue:, start_date:, end_date:)
  (issue[:created_at] > start_date || issue[:updated_at] > start_date) &&
    (issue[:created_at] < end_date || issue[:updated_at] < end_date)
end

def all_issues_within_dates(client:, start_date:, end_date:)
  issues = client.issues('username/reponame', state: 'all')

  last_response = client.last_response

  while last_response.rels[:next]
    last_response = last_response.rels[:next].get
    issues += last_response.data
    break if issues.last[:created_at] < start_date
  end

  issues.select do |issue|
    issue_created_or_updated_within_dates?(
      issue: issue,
      start_date: start_date,
      end_date: end_date
    )
  end
end

def issues_with_label_in_timeline(client:, issues:, labels:)
  issues.select do |issue|
    timeline = client.issue_timeline(
      'username/reponame',
      issue[:number],
      accept: 'application/vnd.github.mockingbird-preview'
    )

    labels_state = labels.each_with_object({}) { |name, hsh| hsh[name] = false }
    timeline.each do |event|
      next if event.label.nil?

      labels_state[event.label.name] = true if labels.include?(event.label.name)
    end

    labels_state.values.all?
  end
end

if $PROGRAM_NAME == __FILE__
  token = ENV['GH_TOKEN']
  if token.nil?
    print 'Enter GitHub personal access token (with repo access, and with SSO enabled): '
    token = gets.chomp
  end

  print 'Enter start date (in the format YYYY-MM-DD): '
  start_date = Time.parse(gets.chomp)
  print 'Enter end date (in the format YYYY-MM-DD): '
  end_date = Time.parse(gets.chomp)
  print 'Enter labels - they must be comma separated (example: P1, security): '
  issue_labels = gets.chomp.split(',').map(&:strip)
  puts 'Searching...'

  client = Octokit::Client.new(access_token: token)
  issues = all_issues_within_dates(
    client: client,
    start_date: start_date,
    end_date: end_date
  )

  puts "Found #{issues.length} issues matching the timeline."
  puts "Searching which issues were ever tagged with these labels: #{issue_labels}"
  labelled_issues = issues_with_label_in_timeline(client: client, issues: issues, labels: issue_labels)
  labelled_issues.each { |issue| puts issue[:number] }
end
