class Book
  include DataMapper::Resource

  property :id, Serial
  property :title, String

  belongs_to :author
end
