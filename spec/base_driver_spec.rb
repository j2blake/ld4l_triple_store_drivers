require 'spec_helper'

module TripleStoreDrivers
  class << self
    def warning(message)
      # disable printed warnings.
    end
  end
end

require_relative 'test_drivers'

describe TripleStoreDrivers::BaseDriver do
  let(:instance) { TestDriver.new({}) }

  describe 'the do_ingest() method' do
    it 'complains if the triple-store is not running' do
      expect {
        instance.do_ingest {}
      }.to raise_error(TripleStoreDrivers::IllegalStateError, /not started/)
    end

    it 'returns an object that will ingest a file of RDF' do
      instance.open
      instance.do_ingest { |i|
        expect(i).to respond_to(:ingest_file)
      }
    end

    it 'complains if called within a do_sparql block' do
      instance.open
      expect {
        instance.do_sparql {
          instance.do_ingest {}
        }
      }.to raise_error(TripleStoreDrivers::IllegalStateError, /do_sparql/)
    end

    it 'complains if called within another do_ingest block' do
      instance.open
      expect {
        instance.do_ingest {
          instance.do_ingest {}
        }
      }.to raise_error(TripleStoreDrivers::IllegalStateError, /do_ingest/)
    end
  end

  describe 'the do_sparql() method' do
    it 'complains if a triple-store is not running' do
      expect {
        instance.do_sparql {}
      }.to raise_error(TripleStoreDrivers::IllegalStateError, /not started/)
    end

    it 'returns an object that will process a sparql query' do
      instance.open
      instance.do_sparql { |s|
        expect(s).to respond_to(:sparql_query)
      }
    end

    it 'complains if called within a do_ingest block' do
      instance.open
      expect {
        instance.do_ingest {
          instance.do_sparql {}
        }
      }.to raise_error(TripleStoreDrivers::IllegalStateError, /do_ingest/)
    end

    it 'complains if called within another do_sparql block' do
      instance.open
      expect {
        instance.do_sparql {
          instance.do_sparql {}
        }
      }.to raise_error(TripleStoreDrivers::IllegalStateError, /do_sparql/)
    end
  end
end