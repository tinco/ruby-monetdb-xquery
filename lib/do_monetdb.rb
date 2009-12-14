require 'data_objects'
#this has to become the gem instead of the .rb I think
require 'MonetDB.rb'

module DataObjects
  module MonetDB
    @monetdb

    attr_reader :connected

    class Connection < DataObjects::Connection
      def initialize(uri_s)
        uri = DataObjects::URI::parse(uri_s)

        @monetdb = MonetDB::MonetDB.new
        username = uri.user
        password = uri.password
        host = uri.host
        port = uri.port
        db_name = uri.path

        lang = query["lang"]
        auth_type = query["auth_type"]

        @monetdb.connect(user, password, lang, host, port, db_name, auth_type)
      end

      def dispose
        @monetdb.close
        @monetdb = nil
      end

      def create_command(text)
        XQueryCommand.new(self, text) if @monetdb.lang == 'xquery'
      end
    end #class Connection

    require 'xmlsimple'
    class XQueryCommand < DataObjects::Command
      def execute_non_query(query)
        @connection.query(query)
      end

      def execute_reader(query)
        result = @connection.query(query)
        data = XmlSimple.xml_in(result.result)
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
