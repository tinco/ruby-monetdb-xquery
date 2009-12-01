require 'data_objects'
#this has to become the gem instead of the .rb I think
require 'MonetDB.rb'

module DataObjects
  module MonetDB
    @monetdb

    attr_reader :connected

    class Connection
      def initialize(uri)
        @monetdb = MonetDB.new
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
    class MonetDBAdapter < DataObjectsAdapter
      def read
        with_connection do |connection|
          command = connection.create_command(statement)
          command.set_types(types)
          result = command.execute_reader(*bind_values)
        end
        result
      end
    end
    const_added (:MonetDBAdapter)
  end
end
