module TripleStoreDrivers
  module BaseDriver
    class << self
      def included(clazz)
        @classes ||= []
          @classes << clazz
      end
      
      def classes
        @classes
      end
      
      private
      
      # Use for unit tests.
      def reset
        @classes = []
      end
    end
    
    #
    # Ask the current instance for an ingester, and yield it to the supplied block.
    # The ingester can be used to ingest files of RDF.
    #
    # Will raise TripleStoreDrivers::IllegalStateError if the instance is not 
    # running, or if this is called from within a do_sparql or another 
    # do_ingest block.
    #
    def do_ingest()
      raise IllegalStateError.new("Triple-store is not started.") unless running?
      raise IllegalStateError.new("Already executing a #{@mode} block") if @mode

      begin
        @mode = :do_ingest
        yield get_ingester
      ensure
        close_ingester
        @mode = nil
      end
    end

    #
    # Ask the current instance for an sparqler, and yield it to the supplied block.
    # The sparqler can be used to service sparql queries.
    #
    # Will raise TripleStoreDrivers::IllegalStateError if the instance is not 
    # running, or if this is called from within another do_sparql or a 
    # do_ingest block.
    #
    def do_sparql()
      raise IllegalStateError.new("Triple-store is not started.") unless running?
      raise IllegalStateError.new("Already executing a #{@mode} block") if @mode

      begin
        @mode = :do_sparql
        yield get_sparqler
      ensure
        close_sparqler
        @mode = nil
      end
    end

  end
end