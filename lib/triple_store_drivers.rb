require 'erb'
require 'json'
require 'net/http'
require 'net/telnet'
require 'ostruct'
require "uri"

require_relative "triple_store_drivers/version"
require_relative "triple_store_drivers/http_handler"

require_relative "triple_store_drivers/base_driver"
require_relative "triple_store_drivers/blaze_graph"
require_relative "triple_store_drivers/virtuoso"

module Kernel
  # Monkey patch for debugging: write a message with a "BOGUS" label to the console.
  def bogus(message)
    puts(">>>>>>>>>>>>>BOGUS #{message}")
  end
end

module TripleStoreDrivers
  # The triple store is not configured correctly.
  class SettingsError < StandardError
  end

  # You screwed up the calling sequence.
  class IllegalStateError < StandardError
  end

  # The triple store didn't do what we wanted or expected.
  class DriverError < StandardError
  end

  # No current settings
  NO_CURRENT_SETTINGS = :no_settings

  # A triple-store with the current settings is running.
  SELECTED_TRIPLE_STORE_RUNNING = :running

  # A triple-store is running, but it doesn't match the current settings.
  # This is the default state, when in doubt.
  OTHER_TRIPLE_STORE_RUNNING = :other_running

  # No triple-store is running.
  NO_TRIPLE_STORE_RUNNING = :not_running

  class Status
    attr_reader :value
    attr_reader :description
    def initialize(value, description)
      @value = value
      @description = description
    end

    def running?
      case value
      when SELECTED_TRIPLE_STORE_RUNNING, OTHER_TRIPLE_STORE_RUNNING
        true
      else
        false
      end
    end

    def to_s
      "#{value} -- #{description}"
    end
  end

  class << self
    #
    # Store these settings for the current triple-store operations.
    #
    # Raises TripleStoreDrivers::SettingsError if the settings are incomplete
    # or inconsistent.
    #
    def select(settings)
      @settings = Hash[settings]
      class_name = @settings[:class_name]
      raise SettingsError.new("Settings must contain a value for :class_name") unless class_name
      begin
        clazz = class_name.split('::').inject(Object) {|o,c| o.const_get c}
        @instance = clazz.new(@settings)
      rescue Exception => e
        @instance = nil
        raise SettingsError.new("Can't create an instance of #{class_name}: #{e.message}")
      end
    end

    #
    # Get a reference to the current triple-store instance.
    #
    # Returns nil if TripleStoreDrivers::select has not been called.
    #
    def selected()
      @instance
    end

    #
    # Find the current status of the triple-store. 'value' will be one of the
    # status constants, 'description' will be text that can be displayed.
    #
    # Are there settings? Is the current instance running? Is another instance running?
    #
    def status()
      return Status.new(NO_CURRENT_SETTINGS, "No settings.") unless @instance
      return Status.new(SELECTED_TRIPLE_STORE_RUNNING, "Running: #{@instance}") if @instance.running?

      BaseDriver.classes.each do |clazz|
        running_instance = clazz.any_running?
        return Status.new(OTHER_TRIPLE_STORE_RUNNING, "Selected: #{@instance}\nRunning: #{running_instance}") if running_instance
      end

      return Status.new(NO_TRIPLE_STORE_RUNNING, "Not running: #{@instance}")
    end

    #
    # Start a triple-store instance using the current settings.
    #
    # If a block is provided, yield the instance of the triple-store, and
    # insure that shutdown will be called. Otherwise, return the instance.
    #
    # Raises TripleStoreDrivers::IllegalStateError if a triple-store is
    # already running. Raises TripleStoreDrivers::DriverError if the
    # triple-store fails to start.
    #
    def startup()
      raise IllegalStateError.new("No triple-store settings.") unless @instance
      s = status
      raise IllegalStateError.new("Stop the triple-store first: #{s.description}") unless s.value == NO_TRIPLE_STORE_RUNNING

      begin
        @instance.open
        if block_given?
          begin
            yield @instance
          ensure
            shutdown
          end
        else
          return @instance
        end
      rescue Exception => e
        raise DriverError.new("Failed to open the triple-store <#{@instance}>: #{e.message}")
      end
    end

    #
    # Stop any running triple-store, regardless of whether it matches the
    # current settings.
    #
    # Raises TripleStoreDrivers::DriverError if the triple-store fails to
    # stop.
    #
    # Will not raise TripleStoreDrivers::IllegalStateError. Call it any time to
    # be sure that no triple-store is running.
    #
    def shutdown()
      begin
        BaseDriver.classes.each do |clazz|
          clazz.close_any
        end
      rescue Exception => e
        warning("Failed to stop the triple-store: #{e.message}")
      end

      raise DriverError.new("Failed to stop the triple-store: status is #{status}") if status.running?()
    end

    def warning(message)
      puts "WARNING: #{message}"
    end

    # --------------------------------------------------------------------------
    private
    # --------------------------------------------------------------------------

    #
    # Forget that we have settings or an instance. Use in unit tests.
    #
    def reset()
      @settings = nil
      @instance = nil
      @mode = nil
      BaseDriver.send(:reset)
    end
  end
end