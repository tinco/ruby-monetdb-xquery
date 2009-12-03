require 'data_objects'
#this has to become the gem instead of the .rb I think
require 'MonetDB.rb'

module DataObjects
  module MonetDB
    @monetdb

    attr_reader :connected

    class Connection
      def initialize(uri)
        @monetdb = MonetDB::MonetDB.new
        username = uri.user
        password = uri.password
        host = uri.host
        port = uri.port
        db_name = uri.path

        lang = query["lang"]
        auth_type = query["auth_type"]

        @connected = @monetdb.connect
      end
    end
  end
end

module DataMapper
  module Adapters
    class MonetDBSQLAdapter < DataObjectsAdapter
    end
    const_added (:MonetDBAdapter)
    class MonetDBXQueryAdapter
    end
  end
end
