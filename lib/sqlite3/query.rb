class SQLite3::Query
  def initialize(sql, statement)
    expect! statement => SQLite3::Statement

    @sql, @statement = sql, statement
  end

  def run(*args)
    # STDERR.puts "Q: #{@sql} #{args.map(&:inspect).join(", ")}"
    @statement.execute *args
  end

  def select(*args)
    @klass ||= SQLite3::Record.for_columns(@statement.columns)

    ary = run(*args).map do |rec|
      @klass.build *rec
    end

    ary.extend Description
    ary.columns = @statement.columns
    ary
  end

  module Description
    attr :columns, true

    # A Google Chart compatible data table; see
    # https://developers.google.com/chart/interactive/docs/reference#dataparam
    def data_table
      cols = columns.map do |column|
        type = case column
        when /_at$/   then :datetime
        when /_on$/   then :date
        when /value/  then :number
        else               :string
        end

        { id: column, type: type, label: column }
      end

      rows = map { |record| convert_record record, cols }

      { cols: cols, rows: rows }
    end

    def convert_record(record, cols)
      values = cols.map do |col|
        id, type = col.values_at(:id, :type)
        v = record.send(id)

        case type
        when :date      then f = v.strftime("%a %b, %Y")
        when :datetime  then f = v.inspect
        when :number    then f = v
        else            f = v
        end


        { v: v, f: f }
      end

      { c: values }
    end
  end

  def ask(*args)
    results = run(*args)
    row = results.first
    results.reset

    if !row               then  nil
    elsif row.length == 1 then  row.first
    else                        row
    end
  end
end
