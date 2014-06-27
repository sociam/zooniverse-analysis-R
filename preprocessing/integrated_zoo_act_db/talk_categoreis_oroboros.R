library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(RMySQL)
library(rjson)

conn <- dbConnect(dbDriver("MySQL"), user = "", password = "", dbname = "zoo_act")


mongo <- mongo.create(host="<server>")

# PH
db <- "zooniverse"
coll <- "discussions"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  value2 <- mongo.cursor.value(curs)
  
  focus <- mongo.bson.value(value2, 'focus.base_type')
  fid <- mongo.bson.value(value2, 'focus._id')
  
  comments <- mongo.bson.value(mongo.cursor.value(curs),'comments')
  
  for(i in comments){
    value <- mongo.bson.from.list(i)
    if(is.null(mongo.bson.value(value, '_id'))){
      actid <- 0
    } else {
      actid <- mongo.oid.to.string(mongo.bson.value(value, '_id'))  
    }
    dbSendQuery(conn, paste("UPDATE zoo_act SET tcat='", focus , "', catid='", fid , "' WHERE actid='",actid,"';",sep=""))
  }
}