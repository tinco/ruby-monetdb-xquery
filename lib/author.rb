class Author
  include DataMapper::Resource

  property :id, Serial
  property :name, String

  has n, :books
end
