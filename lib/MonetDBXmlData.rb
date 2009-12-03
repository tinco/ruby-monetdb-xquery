# The contents of this file are subject to the MonetDB Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is the MonetDB Database System.
#
# The Initial Developer of the Original Code is CWI.
# Portions created by CWI are Copyright (C) 1997-July 2008 CWI.
# Copyright August 2008-2009 MonetDB B.V.
# All Rights Reserved.
# This file has been cedited by Tinco Andringa to support XQuery

# Models a MonetDB RecordSet
require 'MonetDBConnection'
module MonetDB
  class MonetDBXmlData
    @@DEBUG               = false

    attr_reader :errors, :result, :others

    def initialize(connection)
      @connection = connection
    end

    # Fire a query and return the server response
    def execute(q)
      # fire a query and get ready to receive the data
      @connection.send(format_query(q))
      data = @connection.receive

      return if data == nil

      parse_xquery_result(data)
    end

    # Format raw incoming xquery results to clean xml files
    # and throw errors when necessary.
    def parse_xquery_result(data)
      @errors = ""
      @result = ""
      @others = ""
      data.each_line do |line|
        if line[0].chr == '!'
          @errors << line[1..line.length]
        elsif line[0].chr == '='
          @result << line[1..line.length]
        else
          @others << line
        end
      end
      @result.chomp!
    end

    # Free memory used to store the record set
    def free()
      @connection = nil

      @errors = ""
      @result = ""
      @others = ""
    end

    private
    # Formats a query <i>string</i> so that it can be parsed by the server
    def format_query(q)
      return "s#{q}"
    end
  end
end
