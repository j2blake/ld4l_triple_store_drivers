=begin rspec

The common ancestor of all driver classes. It has default implementations of the
methods, and it keeps an array of the classes themselves, so we can call methods
across all of them.

=end

module TripleStoreDrivers
  class BaseDriver
    include LastKnownRunning

    @classes = []
    class << self
      def inherited(subcls)
        @classes << subcls
      end

      def running_instance
        @classes.each do |c|
          description = c.any_running?
          if description
            return description
          end
        end
        return nil
      end

      def stop_any()
        @classes.each {|c| c.close}
      end

      #
      # Each driver class must try to determine whether any instance is running.
      # It returns a description of the running instance, or nil.
      #
      # If not sure that no instances are running, return true.
      #
      def any_running?()
        raise "Subclasses of BaseDriver must define any_running?()"
      end

      #
      # Each driver class must try to close any instance. If the instance cannot
      # be safely closed without loss of data, raise DriverError.
      #
      def close()
        raise "Subclasses of BaseDriver must define close()"
      end

      # Used for unit tests
      private

      def reset()
        @classes = []
      end
    end

    #
    # Each driver instance must be able to tell whether the triple-store is
    # running. This involves more than just detecting the running instance.
    # The current settings must also match the last_known_running.
    #
    # If not sure whether the selected instance actually matches the running
    # triple-store, return false.
    #
    def running?()
      raise "Subclasses of BaseDriver must define running?()"
    end

    #
    # Each driver instance must be able to start its triple-store. If successful
    # it must store the current settings in last_known_running.
    #
    def open()
      raise "Subclasses of BaseDriver must define open()"
    end

    #
    # Each driver must be able to enter an ingest mode. For some, this will
    # have no effect.
    #
    # This yields an ingester to the provided block. The ingester will respond to
    # ingest_file(path, graph_uri)
    #
    def do_ingest()
      raise "Subclasses of BaseDriver must define do_ingest()"
    end

    #
    # Each driver must be able to enter a SPARQL mode. For some, this will
    # have no effect.
    #
    # This yields a sparqler to the provided block. The sparqler will respond to
    # sparql_query(query), yielding an HTTP::Response.
    #
    def do_sparql()
      raise "Subclasses of BaseDriver must define do_sparql()"
    end
  end
end