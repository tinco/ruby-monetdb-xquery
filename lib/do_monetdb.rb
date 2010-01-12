require 'data_objects'
#this has to become the gem instead of the .rb I think
require 'MonetDB.rb'

module DataObjects
  module MonetDB
    @monetdb

    attr_reader :connected
    attr_accessor :lang

    class Connection < DataObjects::Connection
      def initialize(uri_s)
        uri = DataObjects::URI::parse(uri_s)

        @monetdb = MonetDBDriver::MonetDB.new
        username = uri.user || "monetdb"
        password = uri.password || "monetdb"
        host = uri.host || "127.0.0.1"
        port = uri.port || "50000"
        db_name = uri.path || "test"

        lang = uri.query["lang"] || "sql"
        @lang = lang
        auth_type = uri.query["auth_type"] || "SHA1"

        @monetdb.connect(username, password, lang, host, port, db_name, auth_type)
      end

      def dispose
        @monetdb.close
        @monetdb = nil
      end

      def create_command(text)
        XQueryCommand.new(self, text) if @lang == 'xquery'
      end

      def execute(string)
        @monetdb.query(string)
      end
    end #class Connection

    require 'xmlsimple'
    class XQueryCommand < DataObjects::Command
      def execute_non_query(*args)
        @connection.execute(@text)
      end

      def execute_reader(*options)
        result = @connection.execute(@text)
        data = XmlSimple.xml_in(result.result, *options)
        #turn xml into reader
      end

      def set_types(column_types)
        raise NotImplementedError.new
      end
    end #class MonetDBCommand

    class Reader < DataObjects::Reader
      def fields
      end

      def values
      end

      def close
      end

      def next!
      end

      def field_count
      end
    end #class Reader
  end #module MonetDB

  #alias because of capitalisation convention
  module Monetdb
    include MonetDB
  end
end #module DataObjects
