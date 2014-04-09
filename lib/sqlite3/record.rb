# manage SQLite3::Records
#
# The SQLite3::Record module is able to generate classes that are optimized
# for a specific set of columns. It is build on top of Struct, which is way
# faster than Hashes, for example.
module SQLite3::Record
  module ClassMethods
    attr :columns, true

    private

    def to_time(s)
      case s
      when String then Time.parse(s)
      when Fixnum then Time.at(s)
      else s
      end
    end

    def to_date(s)
      return unless time = to_time(s)
      time.to_date
    end

    public

    def build(*attrs)
      attrs = columns.zip(attrs).map do |key, value|
        case key
        when /_at$/ then to_time(value)
        when /_on$/ then to_date(value)
        else value
        end
      end

      new *attrs
    end
  end

  def to_a
    self.class.columns.map do |column| send(column) end
  end

  def to_hash
    kvs = self.class.columns.inject([]) do |ary, column|
      ary << column << send(column)
    end

    Hash[*kvs]
  end

  def self.for_columns(columns)
    columns = columns.map(&:to_sym)

    @@classes ||= {}
    @@classes[columns] ||= begin
      struct = Struct.new(*columns)
      struct.extend SQLite3::Record::ClassMethods
      struct.include SQLite3::Record

      struct.columns = columns
      struct
    end
  end
end
