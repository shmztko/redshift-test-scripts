require 'date'
require 'csv'
require 'json'
require 'logger'

MATCHER_CONFIG = "conf/parse-config.json"

log = Logger.new(STDOUT)
log.level = Logger::INFO


if __FILE__ == $0
  if ARGV.length < 1
    log.warn """
      This script require one arguments.
      0 : (*required) path to parse target file.
    """
    exit 1
  end

  target_file_path = ARGV[0]
  unless File.exists? target_file_path
    log.warn "Parse target file doesn't exists. -> #{target_file_path}"
  end

  # Load configuration file.
  matcher_conf = JSON.parse(File.read(MATCHER_CONFIG), symbolize_names: true)

  # Header
  puts matcher_conf[:export_headers].to_csv

  # Details
  CSV.table(target_file_path).each_with_index{|row, i|
    matched_type = matcher_conf[:query_type_matchers].select{|matcher|
      row[:query_text] =~ Regexp.new(matcher[:regexp])
    }.first

    # get query type
    query_type = matched_type[:type] unless matched_type.nil?

    # get query params
    matched_params = Hash[*matcher_conf[:query_param_matchers].map{|matcher|
      matched = row[:query_text].match(Regexp.new(matcher[:regexp]))

      unless matched.nil?
        [matcher[:name].intern, matched[matcher[:position]]]
      else
        nil
      end
    }.compact.flatten]
    
    # export to stdout
    new_row = []
    matcher_conf[:export_headers].map{|header|header.intern}.each{|header|
      if row[header].nil? == false
        if row[header].is_a? String
          new_row.push(row[header].strip)
        else
          new_row.push(row[header])
        end
      elsif matched_params[header].nil? == false
        if matched_params[header].is_a? String
          new_row.push(matched_params[header].strip)
        else
          new_row.push(matched_params[header])
        end
      elsif header.to_s == 'query_type'
        new_row.push(query_type)
      else
        new_row.push('')
      end
    }
    puts new_row.to_csv
  }
end
