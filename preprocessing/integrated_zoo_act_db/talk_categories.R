library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(RMySQL)
library(rjson)

conn <- dbConnect(dbDriver("MySQL"), user = "", password = "", dbname = "zoo_act")


mongo <- mongo.create(host="<server>")

# PH
db <- "sellers"
coll <- "comments"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  actid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))
  if(!is.null(mongo.bson.value(value2, 'focus_base_type'))){
    dbSendQuery(conn, paste("UPDATE zoo_act SET tcat='", mongo.bson.value(value2, 'focus_base_type') , "', catid='", mongo.bson.value(value2, 'focus_id'), "' WHERE actid='",actid,"';",sep=""))
  }
}

# PH
db <- "andromeda_production"
coll <- "comments"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  actid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))
  if(!is.null(mongo.bson.value(value2, 'focus_base_type'))){
    dbSendQuery(conn, paste("UPDATE zoo_act SET tcat='", mongo.bson.value(value2, 'focus_base_type') , "', catid='", mongo.bson.value(value2, 'focus_id'), "' WHERE actid='",actid,"';",sep=""))
  }
}

# PH 
db <- "oceans_production"
coll <- "comments"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  actid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))
  if(!is.null(mongo.bson.value(value2, 'focus_base_type'))){
    dbSendQuery(conn, paste("UPDATE zoo_act SET tcat='", mongo.bson.value(value2, 'focus_base_type') , "', catid='", mongo.bson.value(value2, 'focus_id'), "' WHERE actid='",actid,"';",sep=""))
  }
}