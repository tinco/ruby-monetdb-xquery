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

      def update(attributes, collection)
        query = collection.query

        properties = []
        bind_values = []

        # make the order of the properties consistent
        query.model.properties(name).each do |property|
          next unless attributes.key?(property)
          properties << property
          bind_values << attributes[property]
        end

        statement, conditions_bind_values = update_statement(properties, query)

        bind_values = conditions_bind_values.concat(bind_values) #reverse order from SQL

        execute(statement, *bind_values).affected_rows
      end

      def delete(collection)
        query = collection.query

        bind_values = []

        statement, conditions_bind_values = delete_statement(query)

        bind_values = conditions_bind_values.concat(bind_values) #reverse order from SQL

        execute(statement, *bind_values).affected_rows
      end

      ## Methods for migrations:
      # @api semipublic
      def upgrade_model_storage(model)
        true #unnecessary with xml
      end

      # @api semipublic
      def create_model_storage(model)
        name = self.name
        storage_name = model.storage_name(name)

        return false if storage_exists?(storage_name)

        url = "http://www.ikweethetniet.nl" #maybe you know.. set up a server and download it from there :P
        execute("pf:add-doc(\"#{url}\", \"#{storage_name}\", \"#{storage_name}\", 10)")
        true
      end

      # @api semipublic
      def destroy_model_storage(model)
        return true unless storage_exists?(model.storage_name(name))
        execute("pf:del-doc(\"#{model.storage_name(self.name)}\")")
        true
      end

      # @api semipublic
      def storage_exists?
        #tsjeck it!
      end
      ##

      module XQuery #:nodoc:
        #TODO rename to something sensible (and research if qualify is necessary at all
        def property_to_column_name(property, qualify)
          column_name = ''

          case qualify
          when true
            column_name << "#{xvar(property.model.storage_name(name))}/"
          when String
            column_name << "#{xvar(qualify)}/"
          end

          column_name << xvar(property.field)
        end

        # Constructs select statement for given query,
        #
        # @return [String] select statement as a string
        #
        # @api private
        def query_statement(query)
          #for $book in doc("books.xml")/books
          #let $info := $book/bookinfo
          #let $title := $info/title,
          #    $isbn := $info/isbn,
          #    $date := $info/pubdate order by $date descending 
          #return if (exists($isbn))
          #then concat($date, " ", $isbn, " ", $title) else ()
          bind_values = []

          model_name = query.model.name.downcase
          statement = "<#{model_name.pluralize}>{"
          statement << "\n"

          #make select clause:
          iteration, bind_values = iterate_statement(query, bind_values)
          statement << iteration
          #return
          statement << "return <#{model_name}>"
          statement << "\n"
          query.fields.each do |property|
            statement << "{ #{xvar(property.name)} } "
            statement << "\n"
          end
          statement << "</#{model_name}>"
          statement << "}</#{model_name.pluralize}>"

          return statement, bind_values
        end

        def iterate_statement(query, bind_values)
          qualify  = query.links.any?
          fields   = query.fields
          order_by = query.order
          group_by = if query.unique?
                       fields.select { |property| property.kind_of?(Property) }
                     end

          model = query.model
          model_name = model.name.downcase

          element = xvar(model_name)
          document ="doc(\"#{query.model.storage_name(name)}\")"

          #for
          statement = "for #{element} in "
          statement << "#{document}/#{model_name.pluralize}/#{model_name} "
          statement << "\n"

          #let
          query.fields.each do |property|
            statement << "let #{xvar(property.name)} := #{element}/#{property.name} "
            statement << "\n"
          end

          #where
          if query.conditions && query.conditions.class != Query::Conditions::NullOperation
            statement << "where "
            conditions, bind_values = conditions_statement(query.conditions)
            statement << conditions
            statement << "\n"
          end

          #order by: statement << "order by $date" if group_by && group_by.any?
          return statement, bind_values
        end

        def insert_statement(model, properties, serial)
          model_name = model.name.downcase
          statement = "do insert "

          statement << "<#{model_name}>"

          if serial
            serial_query = "<#{serial.field}>" #Follows mad ugly auto_increment:
            serial_query << "{ xs:integer(number(fn:max(doc(\"#{model.storage_name(name)}\")"
            serial_query << "/#{model_name.pluralize}/#{model_name}/#{serial.field})) + 1)}"
            serial_query << "</#{serial.field}>"
            statement << serial_query
          end

          #bouw element
          statement << properties.map {|property| "<#{property.field}>?</#{property.field}>"}.join('')

          statement << "</#{model_name}>"

          statement << "as last into doc(\"#{model.storage_name(name)}\")/#{model_name.pluralize}"

          #if supports_returning? && serial
          #  statement << returning_clause(serial)
          #end

          #hij moet eigenlijk ook iets returnen, met rows affected etc.

          statement
        end

        def update_statement(properties, query)
          model = query.model
          name = self.name

          bind_values = []

          iteration, bind_values = iterate_statement(query, bind_values)

          #let $formal := doc("greetings.xml")//greet[1]
          #return
          #(do replace value of $formal/@kind with "impolite",
          # do replace value of $formal with "Bugger Off")
          statement =  "#{iteration} return \n"
          statement << " ( "
          statement << properties.map do |property|
            "do replace value of #{xvar(property.field)} with #{
            property.type == String ? "\"?\"" : "?"}"
          end.join(', ')
          statement << " ) "

          return statement, bind_values
        end

        def delete_statement(query)
          model = query.model
          name = self.name

          bind_values = []

          iteration, bind_values = iterate_statement(query, bind_values)

          #let $book := doc("greetings.xml")//greet[1]
          #return
          #(do delete $book)
          statement =  "#{iteration} return \n"
          statement << " ( do delete  #{xvar(model.name.downcase)} )"

          return statement, bind_values
        end

        def xvar(name)
          '$' + name.to_s
        end

        def conditions_statement(conditions, qualify = false)
          case conditions
          when Query::Conditions::NotOperation       then negate_operation(conditions.operand, qualify)
          when Query::Conditions::AbstractOperation  then operation_statement(conditions, qualify)
          when Query::Conditions::AbstractComparison then comparison_statement(conditions, qualify)
          when Array
            statement, bind_values = conditions  # handle raw conditions
            [ "#{statement}", bind_values ].compact
          end
        end

        def negate_operation(operand, qualify)
          statement = conditions_statement(operand, qualify)
          statement = "not(#{statement})" unless statement.nil?
        end

        def operation_statement(operation, qualify)
          statements  = []
          bind_values = []

          operation.each do |operand|
            statement, values = conditions_statement(operand, qualify)
            next unless statement && values
            statements << statement
            bind_values.concat(values)
          end

          case operation.slug
            when :null then operator = ""
            when :and then operator = "and"
            when :or then operator = "or"
            else
              raise new Exception("Unknown Operation: #{operation.to_s}")
          end

          statement = statements.join(" #{operator} ")

          if statements.size > 1
            statement = "(#{statement})"
          end

          return statement, bind_values
        end

        # Constructs comparison clause
        #
        # @return [String]
        #   comparison clause
        #
        # @api private
        def comparison_statement(comparison, qualify)
          subject = comparison.subject
          value   = comparison.value

          # TODO: move exclusive Range handling into another method, and
          # update conditions_statement to use it

          # break exclusive Range queries up into two comparisons ANDed together
          if value.kind_of? String
            value = "\"#{value}\""
          end

          if value.kind_of?(Range) && value.exclude_end?
            operation = Query::Conditions::Operation.new(
              :and, Query::Conditions::Comparison.new(:gte, subject, value.first),
              Query::Conditions::Comparison.new(:lt,  subject, value.last))

            statement, bind_values = conditions_statement(operation, qualify)

            return "(#{statement})", bind_values
          elsif comparison.relationship?
            if value.respond_to?(:query) && value.respond_to?(:loaded?) && !value.loaded?
              return subquery(value.query, subject, qualify)
            else
              return conditions_statement(comparison.foreign_key_mapping, qualify)
            end
          elsif comparison.slug == :in && value.empty?
            return []  # match everything
          end

          operator    = comparison_operator(comparison)
          column_name = property_to_column_name(subject, qualify)

          # if operator return value contains ? then it means that it is function call
          # and it contains placeholder (%s) for property name as well (used in Oracle adapter for regexp operator)
          if operator.include?('?')
            return operator % column_name, [ value ]
          else
            if value.nil?
              #TODO I assume comparisons with nil are always equals? (not equals apparently is compound)
              return "empty(#{column_name})"
            else
              return "#{column_name}/text() #{operator} ?", [ value ].compact #/text() is an optimalisation (wrt. index generation)
            end
            #return "#{column_name} #{operator} #{value.nil? ? 'NULL' : '?'}", [ value ].compact
          end
        end

        def comparison_operator(comparison)
          subject = comparison.subject
          value   = comparison.value

          case comparison.slug
            when :eql    then '='
            when :in     then include_operator(subject, value)
            when :regexp then 'fn:matches(%s, ?)'
            when :like   then raise Exception.new("Operation: LIKE not supported. Use regexp instead.") #should probably be some dm error type
            when :gt     then '>'
            when :lt     then '<'
            when :gte    then '>='
            when :lte    then '<='
          end
        end

        # @api private
        def include_operator(property, operand)
          case operand
            when Array then '=' #This should work..
            when Range then '=' #apparently ..
          end
        end

        def subquery(query, subject, qualify)
          source_key, target_key = subquery_keys(subject)

          if query.repository.name == name && supports_subquery?(query, source_key, target_key, qualify)
            subquery_statement(query, source_key, target_key, qualify)
          else
            subquery_execute(query, source_key, target_key, qualify)
          end
        end

        # @api private
        def subquery_statement(query, source_key, target_key, qualify)
          query = subquery_query(query, source_key)
          select_statement, bind_values = query_statement(query)

          statement = if target_key.size == 1
                        property_to_column_name(target_key.first, qualify)
                      else
                        "(#{target_key.map { |property| property_to_column_name(property, qualify) }.join(', ')})"
                      end

          statement << " = (#{select_statement})" # was IN

          return statement, bind_values
        end

        # @api private
        def subquery_execute(query, source_key, target_key, qualify)
          query      = subquery_query(query, source_key)
          sources    = query.model.all(query)
          conditions = Query.target_conditions(sources, source_key, target_key)

          if conditions.valid?
            conditions_statement(conditions, qualify)
          else
            [ '1 = 0', [] ]
          end
        end

        # @api private
        def subquery_keys(subject)
          case subject
          when Associations::Relationship
            relationship = subject.inverse
            [ relationship.source_key, relationship.target_key ]
          when PropertySet
            [ subject, subject ]
          end
        end

        # @api private
        def subquery_query(query, source_key)
          # force unique to be false because PostgreSQL has a problem with
          # subselects that contain a GROUP BY with different columns
          # than the outer-most query
          query = query.merge(:fields => source_key, :unique => false)
          query.update(:order => nil) unless query.limit
          query
        end
      end #module XQuery

      include XQuery
    end #MonetdbAdapter
    const_added (:MonetdbAdapter)
  end #Adapters
end #DataMapper
