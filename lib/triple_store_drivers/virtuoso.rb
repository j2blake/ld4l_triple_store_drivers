module TripleStoreDrivers
  class Virtuoso
    include TripleStoreDrivers::BaseDriver
    include TripleStoreDrivers::HttpHandler
    class << self
      def set_instance(instance, settings)
        @instance = instance
        @settings = settings
      end

      def any_running?()
        init_file = get_current_init_file
        return "Virtuoso on #{File.dirname(init_file)}" if init_file
        nil
      end

      def close_any
        if @instance && @instance.running?
          @instance.close
          wait_for_shutdown
        end
        if any_running?
          isql('shutdown;')
          wait_for_shutdown
        end
      end

      def get_current_init_file
        response = `ps -ef | grep virtuoso-[t]`
        if response && response.strip.size > 0
          response.match(/\S+$/)[0]
        else
          nil
        end
      end

      def isql(command)
        output = `isql #{@settings[:isql_port]} #{@settings[:username]} #{@settings[:password]} exec="#{command}" 2>&1`
        error_here = output.index('Error')
        raise output[error_here..-1] if error_here
        output
      end

      def wait_for_shutdown()
        0.step(@settings[:seconds_to_startup], 3) { return if `pgrep virtuoso-t`.size == 0 }
      end
    end

    #
    # All of the parameters have reasonable defaults except the :data_dir.
    #
    DEFAULT_PARAMS = {
      :data_dir => '/DATA/DIR/NOT/SPECIFIED',
      :isql_port => 1111,
      :http_port => 8890,
      :gigs_of_ram => 2,
      :seconds_to_startup => 60,
      :vdb_timeout => 50000,
      :username => 'dba',
      :password => 'dba'}

    def initialize(params)
      @settings = DEFAULT_PARAMS.merge(params)

      @data_dir = @settings[:data_dir]
      if !Dir.exists?(@data_dir) && !Dir.exists?(File.dirname(@data_dir))
        complain("Data directory doesn't exist: #{@data_dir}")
      end

      @isql_port = @settings[:isql_port]
      @http_port = @settings[:http_port]
      @settings[:number_of_buffers] = 85000 * @settings[:gigs_of_ram]
      @settings[:max_dirty_buffers] = 62500 * @settings[:gigs_of_ram]

      self.class.set_instance(self, @settings)
    end

    #
    # It's easy to detect that the process is present, but has it completed
    # initialization? Is it in the process of shutting down? Try repeatedly to
    # connect to both the ISQL port and the HTTP port, to allow it time to either
    # startup or die off.
    #
    # TODO: work from the full ps -ef line, so we can distinguish between this
    # instance and another.
    #
    def running?
      0.step(@settings[:seconds_to_startup], 3) do
        begin
          return false unless `pgrep virtuoso-t`.size > 0
          Net::Telnet::new("Port" => @isql_port, "Timeout" => 2).close
          Net::Telnet::new("Port" => @http_port, "Timeout" => 2).close
          return true
        rescue Exception => e
          sleep 3
        end
      end
      false
    end

    def open
      puts "Opening #{self} \n   #{@settings.to_a.map {|i| "#{i[0]} => #{i[1]}"}.join("\n   ")}}"
      prepare_ini_file

      Dir.chdir(@data_dir) do
        `virtuoso-t -c #{@data_dir}/virtuoso.ini`
        raise "Failed to open Virtuoso: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
        raise "Failed to open Virtuoso -- not running" unless running?
      end

      puts 'Opened.'
    end

    def close
      puts "Closing #{self}."
      isql('shutdown;')
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

    def prepare_ini_file()
      File.open(File.expand_path('virtuoso.ini.template', File.dirname(__FILE__))) do |i|
        File.open("#{@data_dir}/virtuoso.ini", 'w') do |o|
          namespace = OpenStruct.new(@settings)
          o.write(ERB.new(i.read).result(namespace.instance_eval { binding }))
        end
      end
    end

    def isql(command)
      self.class.isql(command)
    end

    def sparql_query(sparql, &block)
      params = {'query' => sparql}
      headers = {'accept' => 'application/sparql-results+json'}
      http_post("http://localhost:#{@http_port}/sparql/", block, params, headers)
    end

    #
    # Since Virtuoso will only ingest files from authorized directories, create a symbolic
    # link in the home directory, and ingest from there.
    #
    # This method of ingesting large files is shown here:
    # https://code.google.com/p/aksw-commons/wiki/Virtuoso_ISQL
    # but it appears to work only for Turtle, NTriples, or RDF/XML.
    #
    def ingest_file(path, graph_uri)
      full_path = File.expand_path(path)
      ext = recognize_extension(path)
      Dir.chdir(@data_dir) do
        `ln -sf #{full_path} ingest_link#{ext}`
        submit_the_ingest_job(ext, graph_uri)
        `rm ingest_link#{ext}`
      end
    end

    def recognize_extension(path)
      ext = File.extname(path)
      if ['.grdf', '.nq', '.nt', '.owl', '.rdf', '.trig', '.ttl', '.xml'].include?(ext)
        ext
      else
        warning("unrecognized extension on '#{path}'")
        '.rdf'
      end
    end

    def submit_the_ingest_job(ext, graph_uri)
      if ['.rdf', '.owl'].include?(ext)
        isql("db.dba.rdf_load_rdfxml(file_to_string_output('ingest_link#{ext}'), '', '#{graph_uri}');")
      else
        isql("ttlp_mt(file_to_string_output('ingest_link#{ext}'), '', '#{graph_uri}');")
      end
    end

    def size()
      return 0 unless running?

      cmd = 'SPARQL SELECT COUNT(*) WHERE {GRAPH  ?g { ?s ?p ?o } '
      cmd << 'FILTER (?g NOT IN (<http://www.openlinksw.com/schemas/virtrdf#>, <http://www.w3.org/ns/ldp#>)) . } ;'
      output = isql(cmd)
      
      output.lines.each do |line|
        return line.to_i if line =~ /^\d+$/
      end
      0
    end

    def clear()
      raise IllegalStateError.new("Clear not permitted on #{self}") unless clear_permitted?
      raise IllegalStateError.new("#{self} is running") if running?

      open
      isql('RDF_GLOBAL_RESET ();')
      isql('delete from DB.DBA.load_list;')
      close
    end

    def to_s()
      @settings[:name] || 'Virtuoso (NO NAME)'
    end
  end
end
