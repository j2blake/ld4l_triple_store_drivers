require 'spec_helper'

describe TripleStoreDrivers do
  before do
    TripleStoreDrivers.send(:reset)
    TripleStoreDrivers::BaseDriver.send(:reset)
  end

  it 'has a version number' do
    expect(TripleStoreDrivers::VERSION).not_to be nil
  end

  context 'the select() method' do
    it 'requires :class_name in the settings' do
      expect {
        TripleStoreDrivers::select(:garbage => 'nothing')
      }.to raise_error(TripleStoreDrivers::SettingsError, /:class_name/)
    end

    it 'requires a valid :class_name in the settings' do
      expect {
        TripleStoreDrivers::select(:class_name => 'TripleStoreDrivers::Bogus')
      }.to raise_error(TripleStoreDrivers::SettingsError, /Can't create/)
    end

    it 'instantiates the named driver class' do
      class DummyDriverClass
        def initialize(params)
        end
      end

      expect {
        TripleStoreDrivers.select(:class_name => 'DummyDriverClass')
      }.to_not raise_error
    end

    it 'instantiates the named driver class, even in a module' do
      module DummyModule
        class DummyDriverClassInModule
          def initialize(params)
          end
        end
      end

      expect {
        TripleStoreDrivers.select(:class_name => 'DummyModule::DummyDriverClassInModule')
      }.to_not raise_error
    end

    it 'smoothly handles an error raised by the driver initialization method.' do
      class BrokenDriverClass
        def initialize(params)
          raise "Some crazy error."
        end
      end

      expect {
        TripleStoreDrivers.select(:class_name => 'BrokenDriverClass')
      }.to raise_error(TripleStoreDrivers::SettingsError, /crazy/)
    end
  end

  context 'the status() method' do
    it 'reacts well to the absence of settings' do
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::NO_CURRENT_SETTINGS)
    end

    it 'knows when the current instance is running' do
      class RunningDriver < TripleStoreDrivers::BaseDriver
        def initialize(junk)
        end

        def running?
          true
        end

        class << self
          def any_running?
            nil
          end
        end
      end

      TripleStoreDrivers.select(:class_name => 'RunningDriver')
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::SELECTED_TRIPLE_STORE_RUNNING)
    end

    it 'knows when some other triple-store is running' do
      class OtherRunningDriver < TripleStoreDrivers::BaseDriver
        def initialize(junk)
        end

        def running?
          false
        end

        def self.any_running?
          'Some silly driver instance.'
        end
      end

      TripleStoreDrivers.select(:class_name => 'OtherRunningDriver')
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::OTHER_TRIPLE_STORE_RUNNING)
    end

    it 'knows when nothing is running.' do
      class NotRunningDriver < TripleStoreDrivers::BaseDriver
        def initialize(junk)
        end

        def running?
          false
        end

        def self.any_running?
          nil
        end
      end

      TripleStoreDrivers.select(:class_name => 'NotRunningDriver')
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::NO_TRIPLE_STORE_RUNNING)
    end
  end

  context 'the startup() method' do
    it 'complains if there are no settings' do
      expect {
        TripleStoreDrivers.startup
      }.to raise_error(TripleStoreDrivers::IllegalStateError)
    end

    it 'complains if a triple-store is already running' do
      class RunningDriver < TripleStoreDrivers::BaseDriver
        def initialize(junk)
        end

        def running?
          true
        end

        class << self
          def any_running?
            nil
          end
        end
      end

      TripleStoreDrivers.select(:class_name => 'RunningDriver')
      expect {
        TripleStoreDrivers.startup
      }.to raise_error(TripleStoreDrivers::IllegalStateError)
    end

    it 'gracefully handles an error from the driver' do
      class FailureDriver < TripleStoreDrivers::BaseDriver
        def initialize(junk)
        end

        def running?
          false
        end

        def open
          raise 'Not openable'
        end

        class << self
          def any_running?
            nil
          end
        end
      end
      
      TripleStoreDrivers.select(:class_name => 'FailureDriver')
      expect {
        TripleStoreDrivers.startup
      }.to raise_error(TripleStoreDrivers::DriverError, /openable/)
    end
  end
end
