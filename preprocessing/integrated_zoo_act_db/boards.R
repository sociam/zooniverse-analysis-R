library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(RMySQL)
library(rjson)

conn <- dbConnect(dbDriver("MySQL"), user = "", password = "", dbname = "zoo_act")


mongo <- mongo.create(host="<server>")

#dbSendQuery(conn, "CREATE TABLE zoo_act_boards (boardid varchar(255), boardlabel varchar(255), parent_board varchar(255));")

# PH
db <- "sellers"
coll <- "boards"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  actid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))
  dbSendQuery(conn, paste("INSERT INTO zoo_act_boards VALUES ('", actid , "','", mongo.bson.value(value2, 'title'),"','", mongo.bson.value(value2, 'board_id'), "');",sep=""))  
}

# PH
db <- "andromeda_production"
coll <- "boards"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  actid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))
  dbSendQuery(conn, paste("INSERT INTO zoo_act_boards VALUES ('", actid , "','", mongo.bson.value(value2, 'title'), "',NULL);",sep=""))
}

# PH 
db <- "oceans_production"
coll <- "boards"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  actid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))
  dbSendQuery(conn, paste("INSERT INTO zoo_act_boards VALUES ('", actid , "','", mongo.bson.value(value2, 'title'), "',NULL);",sep=""))
}

# PH 
db <- "zooniverse"
coll <- "boards"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  actid <- mongo.bson.value(value2, 'zooniverse_id')
  dbSendQuery(conn, paste("INSERT INTO zoo_act_boards VALUES ('", actid , "','", mongo.bson.value(value2, 'title'),"','", mongo.bson.value(value2, 'category'), "');",sep=""))
}