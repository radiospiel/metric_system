class Array
  def by(key = nil, &block)
    ary = []

    if key
      each do |rec|
        ary << rec[key] << rec
      end
    else
      each do |value|
        ary << yield(value) << value
      end
    end

    Hash[*ary]
  end
end
