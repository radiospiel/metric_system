require "json"

class Object
  def to_js
    convert_to_js
  end
  
  alias :convert_to_js :to_json
end

class Array
  def convert_to_js
    "[" + map(&:convert_to_js).join(", ") + "]"
  end
end

class Hash
  def convert_to_js
    "{" + map { |k,v| "#{k.convert_to_js}: #{v.convert_to_js}" }.join(", ") + "}"
  end
end

class Date
  def convert_to_js
    "new Date(#{year}, #{month-1}, #{day})"
  end
end

class Time
  def convert_to_js
    "new Date(#{year}, #{month-1}, #{day}, #{hour}, #{min}, #{sec}, #{usec / 1000})"
  end
end

class Numeric
  def convert_to_js
    "%f" % self
  end
end

class OpenStruct
  def convert_to_js
    @table.to_js
  end
end

if defined?(SQLite3::Record)

module SQLite3::Record::ClassMethods
  def convert_to_js
    to_hash.convert_to_js
  end
end

end
