class StrictTsv
  include Enumerable

  attr_reader :filepath
  def initialize(filepath, *headers)
    @filepath = filepath
    @headers = headers
  end


  def each(&block)
    open(filepath) do |f|
      headers = @headers.empty? ? f.gets.strip.split("\t") : @headers
      f.each do |line|
        fields = Hash[headers.zip(line.split("\t"))]
        block.call fields
      end
    end
  end
end
