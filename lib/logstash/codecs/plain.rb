# encoding: utf-8
require "logstash/codecs/base"
require "logstash/util/charset"

# The "plain" codec is for plain text with no delimiting between events.
#
# This is mainly useful on inputs and outputs that already have a defined
# framing in their transport protocol (such as zeromq, rabbitmq, redis, etc)
class LogStash::Codecs::Plain < LogStash::Codecs::Base
  config_name "plain"

  # Set the message you which to emit for each event. This supports `sprintf`
  # strings.
  #
  # This setting only affects outputs (encoding of events).
  config :format, :validate => :string

  # The character encoding used in this input. Examples include `UTF-8`
  # and `cp1252`
  #
  # This setting is useful if your log files are in `Latin-1` (aka `cp1252`)
  # or in another character set other than `UTF-8`.
  #
  # This only affects "plain" format logs since json is `UTF-8` already.
  config :charset, :validate => ::Encoding.name_list, :default => "UTF-8"

  # The new Java-based event.sprintf method forces the output
  # to be UTF-8 which cannot be undone. Setting `use_legacy_sprintf` to
  # true will revert to using the legacy Ruby-based sprintf method to
  # encode.
  config :use_legacy_sprintf, :validate => :boolean, :default => false

  MESSAGE_FIELD = "message".freeze

  def register
    @converter = LogStash::Util::Charset.new(@charset)
    @converter.logger = @logger
  end

  def decode(data)
    yield LogStash::Event.new(MESSAGE_FIELD => @converter.convert(data))
  end

  # Copied from
  # https://www.rubydoc.info/gems/logstash-event/LogStash/Event#sprintf-instance_method
  def sprintf_legacy(format, event)
    format = format.to_s
    if format.index("%").nil?
      return format
    end

    return format.gsub(/%\{[^}]+\}/) do |tok|
      # Take the inside of the %{ ... }
      key = tok[2 ... -1]

      if key == "+%s"      # Got %{+%s}, support for unix epoch time

        next @data["@timestamp"].to_i
      elsif key[0,1] == "+"
        t = @data["@timestamp"]
        formatter = org.joda.time.format.DateTimeFormat.forPattern(key[1 .. -1])\
          .withZone(org.joda.time.DateTimeZone::UTC)      #next org.joda.time.Instant.new(t.tv_sec * 1000 + t.tv_usec / 1000).toDateTime.toString(formatter)
        # Invoke a specific Instant constructor to avoid this warning in JRuby
        #  > ambiguous Java methods found, using org.joda.time.Instant(long)

        org.joda.time.Instant.java_class.constructor(Java::long).new_instance(
          t.tv_sec * 1000 + t.tv_usec / 1000
        ).to_java.toDateTime.toString(formatter)
      else
        value = event.get(key)
        case value
          when nil
            tok # leave the %{foo} if this field does not exist in this event.
          when Array
            value.join(",") # Join by ',' if value is an array
          when Hash
            value.to_json # Convert hashes to json
          else
            value # otherwise return the value
        end # case value
      end # 'key' checking
    end # format.gsub...
  end

  def encode(event)
    if @format and @use_legacy_sprintf
      encoded = sprintf_legacy(@format, event)
    elsif @format and !@use_legacy_sprintf
      encoded = event.sprintf(@format)
    else
      encoded = event.to_s
    end

    @on_event.call(event, encoded)
  end
end
