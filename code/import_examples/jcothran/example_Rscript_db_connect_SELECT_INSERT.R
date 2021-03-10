print ('start')

library(DBI)

#connect to the database - set password and dbname as needed
pw <- '???'
dbname <- '???'
host <- '???'

#local
#con <- dbConnect(RPostgres::Postgres(), dbname=dbname, user='postgres', password=pw) 
#remote
con <- dbConnect(RPostgres::Postgres(), host=host, port='5432', dbname=dbname, user='postgres', password=pw) 


#listing connected database tables 
dbListTables(con) 

#===================
#setup SQL INSERT query

#qry <- "INSERT INTO areas (name,state) values ('test1','FL')"

#tryCatch( { dbSendQuery(con, qry) }
          
#          , warning = function(w) { print("insert warning:"); print (w); }
#          , error = function(e) { print("insert error:"); print (e); }

#          )

#===================
#setup SQL SELECT query

qry <- "SELECT * FROM areas"

#query result to dataframe - default method
dbGetQuery(con, qry)
df = dbGetQuery(con, qry)
str(df) #df contents

#can setup loop processing using result indexes
df[1,2] #get 1st row, 2nd column
df[2:3,2:3] #get 2-3rd row, 2-3rd column

#===================
#query results to chunk to dataframe - secondary method - like paging/buffering where resultset is larger than memory constraints

#res <- dbSendQuery(con, qry)
#df = dbFetch(res, n = -1) #n = less than zero for all(usual default) or number of rows to return in chunk
#df[1,2] #get 1st row, 2nd column

#===================
#dbClearResult(res) #use with dbSendQuery(INSERT or SELECT chunked results) statements

dbDisconnect(con)
