#TODO: This probably conflicts with ruby-monetdb-sql, so that needs to be
#in the gemspec somehow. I don't know anything about this.
Gem::Specification.new do |s|
   s.required_ruby_version = '>= 1.8.0'
   s.name = %q{ruby-monetdb-mapi}
   s.version = "0.1"
   s.date = %q{2009-11-29}
   s.authors = ["G Modena, T Andringa"]
   s.email = %q{gm@cwi.nl}
   s.summary = %q{Pure Ruby database driver for monetdb5/sql and monetdb5/xquery}
   s.homepage = %q{http://monetdb.cwi.nl/}
   s.description = %q{Pure Ruby database driver for monetdb5/sql and monetdb5/xquery}
   s.files = ["README", "lib/MonetDB.rb", "lib/MonetDBConnection.rb", "lib/MonetDBData.rb", "lib/MonetDBExceptions.rb", "lib/hasher.rb"]
   s.has_rdoc = true
   s.require_path = './lib'
end
