require "csv"
require_relative 'strict_tsv'
class Log_File_Parser
  PRINT_TEMPLATE = "%s:%s\r".freeze
  COUNT_TEMPLATE = "%s total:%s\n".freeze
  SEGMENTED_COUNT_TEMPLATE = "%s total segments:%s\n".freeze
  attr_accessor :file_name

  def initialize(file_path)
    @file_path = file_path
    @file_name = file_path.split('/').last.split('.').first
    @items = Hash.new({})
    @segmenting = nil
    @tsv = StrictTsv.new(file_path,
                         :request_id,
                         :date_stamp,
                         :extra_date,
                         :id,
                         :program_name,
                         :ip,
                         :somthing,
                         :log_level,
                         :app_name,
                         :message )
  end


  def stream
    @tsv.each_with_index do |row, line_num|
      yield row, line_num
    end
  end

  def count_items_by(count_on, ignoring)
    @count_on = count_on
    self.tap do
      if @segmenting
        stream do |row, ln|
          printf(PRINT_TEMPLATE, @file_name, ln) if ln % 1000 == 0
          next if row[@count_on] == ignoring
          next unless segment = segment_by(row)
          @items.fetch(segment) { |key| @items[key] = {} }[row[@count_on]] ||= true
        end
      else
        stream do |row, ln|
          printf(PRINT_TEMPLATE, @file_name, ln)
          @items[row[@count_on]] ||= true unless row[@count_on] == ignoring
        end
      end
      printf(@segmenting ? SEGMENTED_COUNT_TEMPLATE : COUNT_TEMPLATE, @file_name, @items.count)
    end
  end

  def count
    @items.count
  end

  def segment_counts
    @items.map do |segment_by ,segment|

    end
  end

  def write_headers_for(csv)
    if csv.count.eql? 0
      if @segmenting
        csv << ['File name', @segmenting[:name], "unique count of #{@count_on}"]
      else
        csv << ['File name', "unique count of #{@count_on}"]
      end
    end
  end


  def write_to(csv)
    if @segmenting
      @items.each do |segment_by, items|
        csv << [@file_name, segment_by, items.count]
      end
    else
      csv << [@file_name, @items.count]
    end
  end

  def print_first_n_lnes(n=2)
    stream do |l, ln|
      puts l.to_s
      break if ln == n
    end
  end

  def segment_on(section, named_group, regex)
    self.tap do
      @segmenting = { section: section, name: named_group, regex: regex  }
    end
  end

  private

  def segment_by(row)
    row[@segmenting[:section]].match(@segmenting[:regex]).tap do |matches|
      return matches&.[](@segmenting[:name]) || nil
    end
  end
end
