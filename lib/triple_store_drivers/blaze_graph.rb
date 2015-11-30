module TripleStoreDrivers
  class BlazeGraph
    include TripleStoreDrivers::BaseDriver
    include TripleStoreDrivers::HttpHandler
    class << self
      def set_instance(instance, settings)
        @instance = instance
        @settings = settings
      end
    end

    #
    # All of the parameters have reasonable defaults except the :data_dir.
    #
    DEFAULT_PARAMS = {
      :data_dir => '/DATA/DIR/NOT/SPECIFIED',
      :http_port => 9999,
      :gigs_of_ram => 2,
      :seconds_to_startup => 60}

    def initialize(params)
      @settings = DEFAULT_PARAMS.merge(params)

      @data_dir = @settings[:data_dir]
      raise SettingsError.new("Data directory doesn't exist: #{@data_dir}") if !Dir.exists?(File.dirname(@data_dir))
      Dir.mkdir(@data_dir) if !Dir.exists?(@data_dir)

      @http_port = @settings[:http_port]

      self.class.set_instance(self, @settings)
    end

    #
    # If the process is present, try the dummy query. Try repeatedly to
    # allow the process time to either start up or die off.
    #
    # TODO: work from the full ps -ef line, so we can distinguish between this
    # instance and another.
    #
    def running?()
      0.step(@settings[:seconds_to_startup], 3) do
        begin
          return false unless `pgrep -f bigdata-bundled`.size > 0
          sparql_query('SELECT * WHERE { ?s ?p ?o } LIMIT 1') {}
          return true
        rescue Exception => e
          sleep 3
        end
      end
      false
    end

    def open
      begin
        puts "Opening #{self} \n   #{@settings.to_a.map {|i| "#{i[0]} => #{i[1]}"}.join("\n   ")}}"
        prepare_settings_file

        Dir.chdir(@data_dir) do
          cmd = "java -server "
          cmd << "-Xmx#{@settings[:gigs_of_ram]}g "
          cmd << "-Dbigdata.propertyFile=blazegraph.properties "
          cmd << "-jar /Users/jeb228/Downloads/LD4L/BlazeGraph/bigdata-bundled.jar"
          spawn( cmd, :out => 'stdout', :err => 'stderr')
          raise "Failed to open BlazeGraph: exit status = #{$?.exitstatus}" unless $?.exitstatus == 1
          raise "Failed to open BlazeGraph -- not running" unless running?
        end

        puts 'Opened.'
      rescue Exception => e
        bogus e
        bogus e.backtrace.join("\n")
        raise e
      end
    end

    def prepare_settings_file()
      File.open(File.expand_path('blazegraph.properties.template', File.dirname(__FILE__))) do |i|
        File.open("#{@data_dir}/blazegraph.properties", 'w') do |o|
          namespace = OpenStruct.new(@settings)
          o.write(ERB.new(i.read).result(namespace.instance_eval { binding }))
        end
      end
    end

    def close
      puts "Closing #{self}."
      `pkill -f bigdata-bundled`
      raise "Failed to stop #{self}: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
      raise "Failed to stop #{self} -- still running" if running?
      puts 'Closed.'
    end

    def get_ingester
      self
    end

    def ingest_file(path, graph_uri)
      http_post("http://localhost:#{@http_port}/bigdata/sparql", nil, "uri" => "file://#{path}", "context-uri" => graph_uri) { |r| raise_http_error(r) }
    end

    def close_ingester
    end

    def get_sparqler
      self
    end

    def sparql_query(sparql, format='application/sparql-results+json', &block)
      params = {'query' => sparql}
      headers = {'accept' => format}
      http_post("http://localhost:#{@http_port}/sparql", block, params, headers) { |r| raise_http_error(r) }
    end

    def raise_http_error(response)
      exception_lines = response.body.lines.select {|l| l.include?('Exception')}.join("   ").strip
      raise "HTTP Response: %d -- %s\n   %s" % [response.code, response.message, exception_lines]
    end

    def close_sparqler
    end

    def clear
      raise IllegalStateError.new("Clear not permitted") unless clear_permitted?
      raise IllegalStateError.new("#{self} is running") if running?

      Dir.chdir(@data_dir) do
        File.delete('bigdata.jnl')
        File.delete('rules.log')
        File.delete('stdout')
        File.delete('stderr')
      end
    end

    def size()
      return 0 unless running?
      
      sparql_query('SELECT (COUNT(*) as ?count) WHERE { GRAPH ?g { ?s ?p ?o } }') { |r|
        json = JSON.parse(r.body)
        begin
          return json['results']['bindings'][0]['count']['value'].to_s.to_i
        rescue Exception => e
          puts e
        end
      }

      0
    end

    def to_s
      @settings[:name] || 'BlazeGraph (NO NAME)'
    end
  end
end