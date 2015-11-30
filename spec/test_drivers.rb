# Test whether this can be instantiated by name.
class DummyDriverClass
  def initialize(params)
  end
end

# Test whether this can be instantiated by its compound name.
module DummyModule
  class DummyDriverClassInModule
    def initialize(params)
    end
  end
end

# Both the instance and the class change their behavior based on the settings passed to the constructor.
class TestDriver
  include TripleStoreDrivers::BaseDriver
  class << self
    def put_settings(settings, instance)
      @other_running = settings[:test_other_running] || false
      @instance = instance
    end
  end

  def initialize(settings)
    @raise_error_on_open = settings[:test_raise_error_on_open] || false
    @raise_error_on_close = settings[:test_raise_error_on_close] || false
    @fail_to_stop = settings[:test_fail_to_stop] || false
    @running = settings[:test_running] || false

    self.class.put_settings(settings, self)
  end

  def running?
    @running
  end

  def open
    raise 'Problem with un-openable TestDriver' if @raise_error_on_open
    @running = true
  end

  def close
    raise 'Problem with un-closeable TestDriver' if @raise_error_on_close
    @running = false unless @fail_to_stop
  end

  def get_ingester
    @ingester = Object.new
    def @ingester.ingest_file
    end
    @ingester
  end

  def close_ingester
    @ingester = nil
  end

  def get_sparqler
    @sparqler = Object.new
    def @sparqler.sparql_query
    end
    @sparqler
  end

  def close_sparqler
    @sparqler = nil
  end
end
