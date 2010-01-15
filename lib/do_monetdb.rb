require 'data_objects'
#this has to become the gem instead of the .rb I think
require 'MonetDB.rb'

module DataObjects
  module MonetDB
    @monetdb

    attr_reader :connected
    attr_accessor :lang

    class Connection < DataObjects::Connection
      #Example: monetdb://localhost:51016?lang=xquery
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

    class XQueryCommand < DataObjects::Command
      def execute_non_query(*args)
        reader = execute_reader
        insert_id = reader.result["InsertID"]
        affected_rows = reader.result["AffectedRows"]
        new Result(self, affected_rows, insert_id)
      end

      def execute_reader(*options)
        result = @connection.execute(@text)
        reader = XQueryReader.new(@column_types, *options)
        reader.read(result.result)
        reader
      end

      def set_types(column_types)
        @column_types = column_types
        #raise NotImplementedError.new
      end
    end #class MonetDBCommand

    require 'xmlsimple'
    class XQueryReader < DataObjects::Reader
      def initialize(column_types, *options)
        @column_types = column_types
        @options = options
      end

      def read(xml)
        @result = XmlSimple.xml_in(xml, 'ForceArray' => false, *@options)
        @modelname = @result.keys.first
        @position = -1
      end

      def result
        @result
      end

      def fields
        @result[@modelname][0].keys
      end

      def values
        fields.collect do |f| @result[@modelname][@position][f] end
      end

      def close
        @result = nil
      end

      def next!
        @position += 1
        not @result[@modelname][@position].nil? || nil
      end

      def field_count
        fields.count
      end

      def each
        @result[@modelname].each do |row|
          yield row
        end
      end
    end #class Reader
  end #module MonetDB

  #alias because of capitalisation convention
  module Monetdb
    include MonetDB
  end
end #module DataObjects
