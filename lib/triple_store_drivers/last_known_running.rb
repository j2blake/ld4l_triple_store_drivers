module TripleStoreDrivers
  module LastKnownRunning
    LAST_KNOWN_FILE = File.expand_path('~/.triple_store_drivers/last_known_running')
    
    def match_last_known?(params)
      return false unless File.exist?(LAST_KNOWN_FILE)
      return params.to_s == File.read(LAST_KNOWN_FILE)
    end

    def clear_last_known()
      if File.exist?(LAST_KNOWN_FILE)
        File.delete(LAST_KNOWN_FILE)
      end
    end
    
    def set_last_known(params)
      dirname = File.dirname(LAST_KNOWN_FILE)
      Dir.mkdir(dirname) unless Dir.exist?(dirname)
      File.open(LAST_KNOWN_FILE, 'w') {|f| f.write(params.to_s) }
    end
  end
end