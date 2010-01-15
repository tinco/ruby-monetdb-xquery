require 'datamapper'
require 'data_objects'
require DataMapper.root / 'lib' / 'dm-core' / 'adapters' / 'data_objects_adapter'
require 'do_monetdb.rb'

module DataMapper
  module Adapters
    class MonetdbAdapter < DataObjectsAdapter
      # Constructs and executes SELECT query, then instantiates
      # one or many object from result set.
      #
      # @param [Query] query
      #   composition of the query to perform
      #
      # @return [Array]
      #   result set of the query
      #
      # @api semipublic
      def read(query)
        fields = query.fields
        types  = fields.map { |property| property.primitive }

        statement, bind_values = query_statement(query)

        records = []

        with_connection do |connection|
          command = connection.create_command(statement)
          command.set_types(types)

          reader = command.execute_reader(*bind_values)

          records = reader.entries
        end

        records
      end

      module XQuery #:nodoc:

        # Constructs select statement for given query,
        #
        # @return [String] select statement as a string
        #
        # @api private
        def query_statement(query)
          qualify  = query.links.any?
          fields   = query.fields
          order_by = query.order
          group_by = if query.unique?
                       fields.select { |property| property.kind_of?(Property) }
                     end
          model = query.model
          model_name = model.name.downcase

          #for $book in doc("books.xml")/books
          #let $info := $book/bookinfo
          #let $title := $info/title,
          #    $isbn := $info/isbn,
          #    $date := $info/pubdate order by $date descending 
          #return if (exists($isbn))
          #then concat($date, " ", $isbn, " ", $title) else ()

          bind_values = []

          statement = "<#{model_name.pluralize}>{"
          document ="doc(\"#{query.model.storage_name(name)}\")"
          element = xvar(model_name)
          statement << "for #{element} in "
          statement << "#{document}/#{model_name.pluralize}/#{model_name} " #from
          #eventuele joins
          query.fields.each do |property|
            statement << "let #{xvar(property.name)} := #{element}/#{property.name} "
          end
          #order by: statement << "order by $date" if group_by && group_by.any?
          statement << "return <#{model_name}>"
          query.fields.each do |property|
            statement << "{ #{xvar(property.name)} } "
          end
          statement << "</#{model_name}>"
          statement << "}</#{model_name.pluralize}>"

          return statement, bind_values
        end

        def xvar(name)
          '$' + name.to_s
        end
      end #module XQuery

      include XQuery
    end
    const_added (:MonetdbAdapter)
  end
end

