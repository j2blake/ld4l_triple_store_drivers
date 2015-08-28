module NonModalDriver
  @mode = :none
  
  def do_ingest()
    set_mode(:none, :ingest)
    yield(self)
    set_mode(:ingest, :none)
  end

  def do_sparql()
    set_mode(:none, :sparql)
    yield(self)
    set_mode(:sparql, :none)
  end

  def assert_mode(expected)
    raise("Wrong mode: expected #{expected}, but found #{@mode}") unless expected == @mode
    
  end
  def set_mode(was, will_be)
    assert_mode(was)
    @mode = will_be
  end
end