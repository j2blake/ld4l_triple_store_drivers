=begin
--------------------------------------------------------------------------------

This is a little funky because 4store runs as two separate processes, and it's
not really running unless they are both present. On the other hand, it's not
really stopped unless they are both absent.

--------------------------------------------------------------------------------

For size, go to http://localhost:8000/status/size/

--------------------------------------------------------------------------------
=end

module TripleStoreDrivers
  class FourStore
    include TripleStoreDrivers::BaseDriver
    class << self
      def set_instance(instance, settings)
        @instance = instance
        @settings = settings
      end

      def any_running?()
        # This is bogus but will work for now.
        @instance && @instance.running?
      end

      def close_any
        if @instance && @instance.running?
          @instance.close
          wait_for_shutdown
        end
        if any_running?
          wait_for_shutdown
        end
      end

      def wait_for_shutdown()
        0.step(@settings[:seconds_to_startup], 3) { return unless any_running? }
      end
    end

    #
    # All of the parameters have reasonable defaults.
    #
    DEFAULT_PARAMS = {
      :base_data_dir => '/var/lib/4store',
      :data_dir => '/ACTUAL/DATA/DIRECTORY/NOT/SPECIFIED',
      :seconds_to_startup => 30,
      :db_name => 'mydb',
      :http_port => 8000
    }

    def initialize(params)
      @settings = DEFAULT_PARAMS.merge(params)

      @constructor = Constructor.new(@settings[:db_name], @settings[:data_dir], @settings[:base_data_dir])
      @backend = Backend.new(@settings[:db_name])
      @httpd = Httpd.new(@settings[:http_port], @settings[:db_name])

      self.class.set_instance(self, @settings)
    end

    def running?()
      return false unless @backend.running?
      return false unless @httpd.running?
      true
    end

    def open
      puts "Opening #{self} \n   #{format_settings}}"
      @backend.open
      @httpd.open
      if running?
        puts 'Opened.'
      else
        raise "Failed to start #{self}"
      end
    end

    def format_settings
      @settings.to_a.map {|i| "#{i[0]} => #{i[1]}"}.join("\n   ")
    end

    def close
      puts "Closing #{self}."
      @httpd.close
      @backend.close
      if running?
        raise "Failed to close #{self} -- still running"
      else
        puts 'Closed.'
      end

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
      @httpd.sparql_query(sparql, format, &block)
    end

    def sparql_update(sparql, &block)
      @httpd.sparql_update(sparql, &block)
    end

    def ingest_file(path, graph_uri)
      @httpd.sparql_update("LOAD <file://#{path}> INTO GRAPH <#{graph_uri}>") {}
    end

    def size()
      @httpd.size
    end

    def clear()
      raise IllegalStateError.new("Clear not permitted on #{self}") unless clear_permitted?
      raise IllegalStateError.new("#{self} is running") if running?
      @constructor.destroy
      @constructor.create
    end

    def to_s()
      @settings[:name] || '4store (NO NAME)'
    end
  end

  class Constructor
    def initialize(db_name, data_dir, base_data_dir)
      @db_name = db_name
      @data_dir = data_dir
      @base_data_dir = base_data_dir
      raise IllegalStateError.new("Data directory doesn't exist: #{@data_dir}") unless Dir.exists?(@data_dir)
    end

    def create
      link_to = File.expand_path(@db_name, @data_dir)
      link = File.expand_path(@db_name, @base_data_dir)

      raise IllegalStateError.new("Data directory already exists: #{link_to}") if Dir.exists?(link_to)
      raise IllegalStateError.new("Link to data directory already exists: #{link}") if Dir.exists?(link)

      puts `4s-backend-setup #{@db_name}`
      raise "4s-backend-setup failed: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0

      `mv #{link} #{link_to}`
      raise "Failed to move the data directory: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0

      puts `ln -s #{link_to} #{link}`
      raise "Failed to create the link: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
    end

    def destroy
      FileUtils.rm_r(File.expand_path(@db_name, @data_dir))
      FileUtils.rm(File.expand_path(@db_name, @base_data_dir))
    end
  end

  class Backend
    def initialize(db_name)
      @db_name = db_name
    end

    def running?
      return false if `pgrep 4s-backend`.empty?
      return true
    end

    def open
      unless running?
        puts `4s-backend #{@db_name}`
        raise "Failed to start 4store backend: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
      end
    end

    def close
      if running?
        puts `pkill 4s-backend`
        raise "Failed to stop 4store backend: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
      end
    end
  end

  class Httpd
    include TripleStoreDrivers::HttpHandler
    def initialize(http_port, db_name)
      @http_port = http_port
      @db_name = db_name
    end

    def running?
      (1..10).each do
        begin
          return false if `pgrep 4s-httpd`.empty?
          sparql_query('SELECT ?s WHERE {?s ?p ?o} LIMIT 1') {}
          return true
        rescue Exception => e
          bogus e
          sleep 2
        end
      end
      false
    end

    def open
      unless running?
        puts `4s-httpd -p #{@http_port} -s -1 #{@db_name}`
        raise "Failed to start 4store frontend: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
      end
    end

    def close
      unless `pgrep 4s-httpd`.empty?
        puts `pkill 4s-httpd`
        raise "Failed to stop 4store frontend: exit status = #{$?.exitstatus}" unless $?.exitstatus == 0
      end
    end

    def size
      return 0 unless running?
      block = Proc.new { |resp|
        return 0 unless resp.body =~ /Total[dht\/<>]+(\d+)/m # <th>Total</th><td>20000000</td>
        return $1.to_i
      }
      http_get("http://localhost:#{@http_port}/status/size/", block, {}, {})
    end

    def sparql_query(sparql, format='application/sparql-results+json', &block)
      params = {'query' => sparql}
      headers = {'accept' => format}
      http_post("http://localhost:#{@http_port}/sparql/", block, params, headers)
    end

    def sparql_update(sparql, &block)
      params = {'update' => sparql}
      headers = {}
      http_post("http://localhost:#{@http_port}/update/", block, params, headers)
    end

  end

end
