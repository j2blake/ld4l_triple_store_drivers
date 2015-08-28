module TripleStoreDrivers
  class Virtuoso < BaseDriver
    class <<self
      def any_running?()
        return false unless `pgrep virtuoso-t`.size > 0
      end

      def close()
        if running?
          puts "Closing Virtuoso."
          isql('shutdown;')
          raise "Failed to stop Virtuoso" if running?
          clear_last_known
          puts "Closed."
        end
        # Is this right? Perhaps we should just give up, to avoid losing data.
        # Can we try isql('shutdown', get_last_known)
        #        if any_running?
        #          puts "Closing another Virtuoso."
        #          `pkill virtuoso-t`
        #          sleep 3
        raise "Failed to stop another Virtuoso" if any_running?
        #          clear_last_known
        #          puts "Closed."
        #        end
      end
    end

    include NonModalDriver
    include HttpHandler

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
      @params = DEFAULT_PARAMS.merge(params)

      @data_dir = @params[:data_dir]
      if !Dir.exists?(@data_dir) && !Dir.exists?(File.dirname(@data_dir))
        complain("Data directory doesn't exist: #{@data_dir}")
      end

      @isql_port = @params[:isql_port]
      @http_port = @params[:http_port]

      @params[:number_of_buffers] = 85000 * @params[:gigs_of_ram]
      @params[:max_dirty_buffers] = 62500 * @params[:gigs_of_ram]

      @ingesting = false
    end

    def running?()
      #
      # It's easy to detect that the process is present, but has it completed
      # initialization? Is it in the process of shutting down? Try repeatedly to
      # connect to both the ISQL port and the HTTP port, to allow it time to either
      # startup or die off.
      #
      def running?()
        return false unless match_last_known?(@params)

        0.step(@params[:seconds_to_startup], 3) do
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
    end

    def open()
      puts "Opening Virtuoso in #{@data_dir} \n   #{@params.inspect}"

      clear_last_known

      prepare_ini_file

      Dir.chdir(@data_dir) do
        `virtuoso-t -c #{@data_dir}/virtuoso.ini`
        raise "Failed to open Virtuoso: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
      end

      set_last_known(@params)

      unless running?
        raise "Failed to open Virtuoso"
        clear_last_known
      end

      puts 'Opened Virtuoso.'
    end

    def prepare_ini_file()
      File.open(File.expand_path('virtuoso.ini.template', File.dirname(__FILE__))) do |i|
        File.open("#{@data_dir}/virtuoso.ini", 'w') do |o|
          namespace = OpenStruct.new(@params)
          o.write(ERB.new(i.read).result(namespace.instance_eval { binding }))
        end
      end
    end

    def isql(command, params=@params)
      output = `isql #{params[:isql_port]} #{params[:username]} #{params[:password]} exec="#{command}"`
      if output.include?('Error')
        raise output
      end
    end

    def sparql_query(sparql, &block)
      assert_mode(:sparql)
      params = {'query' => sparql}
      headers = {'accept' => 'application/sparql-results+json'}
      http_post("http://localhost:#{@http_port}/sparql/", block, params, headers)
    end

    # Need to use the bulk ingest method in order to load a file larger than 10 MBytes
    #
    # Since Virtuoso will only ingest files from authorized directories, create a symbolic
    # link in the home directory, and ingest from there.
    #
    # Since Virtuoso keeps a record of the files that is has already ingested, we need to
    # clear that record in order to load repeatedly from the same link.
    def ingest_file(path, graph_uri)
      assert_mode(:ingest)
      ext = recognize_extension(path)
      Dir.chdir(@data_dir) do
        `ln -sf #{path} ingest_link#{ext}`
        isql('delete from DB.DBA.load_list;')
        isql("ld_dir('#{@data_dir}', 'ingest_link#{ext}', '#{graph_uri}');")
        isql('rdf_loader_run();')
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

    def to_s
      "Virtuoso on #{@data_dir}"
    end
  end
end
