=begin

This will report counts and timings on the TripleStore driver that it wraps.

Provide the class of the delegate as delegate_class_name, and all of the
parameters that the delegate will require.

=end

module TripleStoreDrivers
  class InstrumentedWrapper
    include TripleStoreDrivers::BaseDriver
    class << self
      def set_instance(instance, settings)
        @instance = instance
        @settings = settings
      end
    end

    DEFAULT_PARAMS = {
      :delegate_class_name => 'DELEGATE::CLASS::NOT::PROVIDED'
    }

    def initialize(params)
      @timings = Timings.new
      @settings = DEFAULT_PARAMS.merge(params)
      class_name = @settings[:delegate_class_name]
      begin
        clazz = class_name.split('::').inject(Object) {|o,c| o.const_get c}
        @delegate = clazz.new(@settings)
      rescue Exception => e
        @delegate = nil
        raise SettingsError.new("InstrumentedWrapper: can't create a delegate instance of #{class_name}: #{e.message}")
      end

      self.class.set_instance(self, @settings)
    end

    def running?
      @delegate.running?
    end

    def open
      @delegate.open
    end

    def close
      @delegate.close
    end

    def get_ingester
      self
    end

    def close_ingester
    end

    def get_sparqler
      self
    end

    def close_sparqler
    end

    def sparql_query(sparql, format='application/sparql-results+json', &block)
      result = nil
      elapsed = Benchmark.realtime do
        result = @delegate.sparql_query(sparql, format, &block)
      end
      @timings.record_sparql_query(sparql, format, elapsed)
      result
    end

    def ingest_file(path, graph_uri)
      elapsed = Benchmark.realtime do
        result = @delegate.ingest_file(path, graph_uri)
      end
      @timings.record_ingest_file(path, graph_uri, elapsed)
      result
    end

    def size()
      @delegate.size
    end

    def clear()
      @delegate.clear
    end

    def to_s()
      "Wrapper around #{@delegate}"
    end
  end

  class Timings
    class Bucket
      attr_reader :key
      attr_reader :count
      attr_reader :total_time
      def initialize(key)
        @key = key
        @count = 0
        @total_time = 0.0
      end

      def record(elapsed)
        @count += 1
        @total_time += elapsed
      end

      def <=>(anOther)
        @total_time <=> anOther.total_time
      end
    end

    def initialize()
      at_exit { report }
      @buckets = Hash.new() { |hash, key| hash[key] = Bucket.new(key) }
      @start_time = Time.now
    end

    def record_ingest_file(path, graph_uri, elapsed)
      record('INGEST FILE', elapsed)
    end

    def record_sparql_query(sparql, format, elapsed)
      record(genericize_query(sparql, format), elapsed)
    end

    def genericize_query(sparql, format)
      sparql.gsub(/PREFIX.*?$/, '').gsub(/<.+?>/, '<...>').gsub(/\s+/, ' ').gsub(/OFFSET \d+/, 'OFFSET xxxx')
    end

    def record(key, elapsed)
      if @buckets.size < 50
        @buckets[key].record(elapsed)
      else
        @buckets['OTHER'].record(elapsed)
      end
    end

    def report
      array = @buckets.values.sort.reverse
      total_seconds = Time.now.to_i - @start_time.to_i
      total_bucketed = array.inject(0.0) { |sum, bucket| sum + bucket.total_time }.to_i

      puts "TOTAL CLOCK TIME:        %10d seconds" % [total_seconds]
      puts "TOTAL QUERY/INGEST TIME: %10d seconds" % [total_bucketed]
      puts
      array.each do |bucket|
        puts "    Total    Count Average Bucket"
        puts "%9d %8d %7.2f %s" % [bucket.total_time.to_i, bucket.count, bucket.total_time / bucket.count, bucket.key]
      end
    end
  end
end
