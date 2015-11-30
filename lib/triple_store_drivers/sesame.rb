module TripleStoreDrivers
  class Sesame
    include TripleStoreDrivers::BaseDriver
    include TripleStoreDrivers::HttpHandler
    SESAME_HOME = '/Users/jeb228/TripleStores/Sesame'
    SESAME_APACHE_HOME = SESAME_HOME + '/apache-tomcat-8.0.23'
    SESAME_DATA_ROOT = SESAME_HOME + '/data/OpenRDF Sesame/repositories'
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
      :seconds_to_startup => 30
    }

    def initialize(params)
      @settings = DEFAULT_PARAMS.merge(params)
      self.class.set_instance(self, @settings)
    end

    def running?()
      (1..10).each do
        begin
          return false unless `pgrep -f Sesame/apache-tomcat`.size > 0
          http_post('http://localhost:8080/openrdf-sesame/home/overview.view', Proc.new{}, {}, {})
          return true
        rescue Exception => e
          sleep 2
        end
      end
      false
    end

    def open
      puts "Opening #{self} \n   #{@settings.to_a.map {|i| "#{i[0]} => #{i[1]}"}.join("\n   ")}}"
      Dir.chdir(SESAME_APACHE_HOME) do
        `bin/startup.sh`
        raise "Failed to start Sesame" unless running?
      end
      puts 'Opened.'
    end

    def close
      puts "Closing #{self}."
      Dir.chdir(SESAME_APACHE_HOME) do
        `bin/shutdown.sh`
      end
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
      http_post('http://localhost:8080/openrdf-sesame/repositories/myRepo', block, params, headers)
    end

    def ingest_file(path, graph_uri)
      sparql_update("LOAD <file://#{path}> INTO GRAPH <#{graph_uri}>") {}
    end

    def sparql_update(sparql, &block)
      params = {'update' => sparql}
      headers = {}
      http_post('http://localhost:8080/openrdf-sesame/repositories/myRepo/statements', block, params, headers) 
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

      Dir.chdir(SESAME_DATA_ROOT + '/myRepo') do |dir|
        Dir.entries(dir).each do |fname|
          File.delete(fname) unless fname.start_with?('.')
        end
      end
    end

    def to_s()
      @settings[:name] || 'Sesame (NO NAME)'
    end
  end
end
