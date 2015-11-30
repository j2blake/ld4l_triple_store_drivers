require 'spec_helper'

module TripleStoreDrivers
  class << self
    def warning(message)
      # disable printed warnings.
    end
  end
end

describe TripleStoreDrivers do
  before do
    TripleStoreDrivers.send(:reset)
    load 'test_drivers.rb'
  end

  it 'has a version number' do
    expect(TripleStoreDrivers::VERSION).not_to be(nil)
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
      TripleStoreDrivers.select(:class_name => 'DummyDriverClass')
      expect(TripleStoreDrivers.selected.class).to be(DummyDriverClass)
    end

    it 'instantiates the named driver class, even in a module' do
      TripleStoreDrivers.select(:class_name => 'DummyModule::DummyDriverClassInModule')
      expect(TripleStoreDrivers.selected.class).to be(DummyModule::DummyDriverClassInModule)
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

  describe 'the status() method' do
    it 'reacts well to the absence of settings' do
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::NO_CURRENT_SETTINGS)
    end

    it 'knows when the current instance is running' do
      TripleStoreDrivers.select(:class_name => 'TestDriver', :test_running => true)
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::SELECTED_TRIPLE_STORE_RUNNING)
    end

    it 'knows when nothing is running.' do
      TripleStoreDrivers.select(:class_name => 'TestDriver')
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::NO_TRIPLE_STORE_RUNNING)
    end
  end

  describe 'the startup() method' do
    it 'complains if there are no settings' do
      expect {
        TripleStoreDrivers.startup
      }.to raise_error(TripleStoreDrivers::IllegalStateError)
    end

    it 'starts if a triple-store is not already running' do
      TripleStoreDrivers.select(:class_name => 'TestDriver')
      expect {
        TripleStoreDrivers.startup
      }.to_not raise_error
    end

    it 'complains if a triple-store is already running' do
      TripleStoreDrivers.select(:class_name => 'TestDriver', :test_running => true)
      expect {
        TripleStoreDrivers.startup
      }.to raise_error(TripleStoreDrivers::IllegalStateError)
    end

    it 'gracefully handles an error from the driver' do
      TripleStoreDrivers.select(:class_name => 'TestDriver', :test_raise_error_on_open => true)
      expect {
        TripleStoreDrivers.startup
      }.to raise_error(TripleStoreDrivers::DriverError, /openable/)
    end
  end

  describe 'the shutdown() method' do
    it 'doesn\'t object if there are no settings' do
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::NO_CURRENT_SETTINGS)
      expect {
        TripleStoreDrivers.shutdown
      }.to_not raise_error
    end

    it 'doesn\'t object if no triple-store is running' do
      TripleStoreDrivers.select(:class_name => 'TestDriver')
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::NO_TRIPLE_STORE_RUNNING)
      expect {
        TripleStoreDrivers.shutdown
      }.to_not raise_error
    end

    it 'stops a triple-store if it is running' do
      TripleStoreDrivers.select(:class_name => 'TestDriver', :test_running => true)
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::SELECTED_TRIPLE_STORE_RUNNING)
      TripleStoreDrivers.shutdown
      expect(TripleStoreDrivers.status.value).to eq(TripleStoreDrivers::NO_TRIPLE_STORE_RUNNING)
    end

    it 'gracefully handles an error from the driver' do
      TripleStoreDrivers.select(:class_name => 'TestDriver', :test_running => true, :test_raise_error_on_close => true)
      expect {
        TripleStoreDrivers.shutdown
      }.to raise_error(TripleStoreDrivers::DriverError, /Failed to stop/)
    end

    it 'gracefully handles the failure to stop a triple-store' do
      TripleStoreDrivers.select(:class_name => 'TestDriver', :test_running => true, :test_fail_to_stop => true)
      expect {
        TripleStoreDrivers.shutdown
      }.to raise_error(TripleStoreDrivers::DriverError, /Failed to stop/)
    end
  end

end
