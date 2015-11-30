module TripleStoreDrivers
  class Fuseki2
    include TripleStoreDrivers::BaseDriver
    include TripleStoreDrivers::HttpHandler
    FUSEKI2_HOME = '/Users/jeb228/Downloads/LD4L/Fuseki/apache-jena-fuseki-2.0.0'
    class << self
      def set_instance(instance, settings)
        @instance = instance
        @settings = settings
      end
    end

    #
    # All of the parameters have reasonable defaults.
    #
    DEFAULT_PARAMS = {
      :data_dir => 'NO DATA DIRECTORY',
      :http_port => 3030,
      :dataset_name => 'ds',
      :seconds_to_startup => 30
    }

    def initialize(params)
      @settings = DEFAULT_PARAMS.merge(params)

      @data_dir = @settings[:data_dir]
      if !Dir.exists?(@data_dir)
        raise IllegalStateError.new("Data directory doesn't exist: #{@data_dir}")
      end

      @http_port = @settings[:http_port]
      @dataset_name = @settings[:dataset_name]

      self.class.set_instance(self, @settings)
    end

    def running?()
      (1..10).each do
        begin
          return false unless `pgrep -f Fuseki2/myDataset`.size > 0
          sparql_query('SELECT 1 WHERE {}') {}
          return true
        rescue Exception => e
          sleep 2
        end
      end
      false
    end

    def open
      puts "Opening #{self} \n   #{@settings.to_a.map {|i| "#{i[0]} => #{i[1]}"}.join("\n   ")}}"
      Dir.chdir(FUSEKI2_HOME) do
        spawn("./fuseki-server --update --loc=#{@data_dir} /#{@dataset_name}", :out => '/dev/null', :err => '/dev/null')
        raise "Failed to start Fuseki2" unless running?
      end

      puts 'Opened.'
    end

    def close
      puts "Closing #{self}."
      puts `pkill -f fuseki-server`
      raise "Failed to close #{self}: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
      raise "Failed to close #{self} -- still running" if running?
      puts 'Closed.'
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
      params = {'query' => sparql}
      headers = {'accept' => format}
      http_post("http://localhost:#{@http_port}/#{@dataset_name}/query", block, params, headers)
    end

    def ingest_file(path, graph_uri)
      sparql_update("LOAD <file://#{path}> INTO GRAPH <#{graph_uri}>") {}
    end

    def sparql_update(sparql, &block)
      params = {'update' => sparql}
      headers = {}
      http_post("http://localhost:#{@http_port}/#{@dataset_name}/update", block, params, headers) 
    end

    def size()
      return 0 unless running?
      sparql_query("SELECT (count(*) as ?count) WHERE { GRAPH ?g { ?s ?p ?o } }") do |resp|
        return JSON.parse(resp.body)['results']['bindings'][0]['count']['value'].to_i
      end
    end

    def clear()
      raise IllegalStateError.new("Clear not permitted on #{self}") unless clear_permitted?
      raise IllegalStateError.new("#{self} is running") if running?

      Dir.chdir(@data_dir) do |dir|
        Dir.entries(dir).each do |fname|
          File.delete(fname) unless fname.start_with?('.')
        end
      end
    end

    def to_s()
      @settings[:name] || 'Fuseki2 (NO NAME)'
    end
  end
end
