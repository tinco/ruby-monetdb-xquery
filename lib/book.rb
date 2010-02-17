class Book
  include DataMapper::Resource
  storage_names[:default] = 'books.xml'

  property :id, Serial
  property :title, String
end

