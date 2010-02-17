require 'monetdb_adapter'
dm = DataMapper.setup(:default, "monetdb://localhost:51016?lang=xquery")
