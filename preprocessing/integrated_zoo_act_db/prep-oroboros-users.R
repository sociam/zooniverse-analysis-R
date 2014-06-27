library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(rjson)

conn <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost/voc",uid="bokker",pwd="bokker")

#create the user db
dbSendQuery(conn, "CREATE TABLE zoo_users (zooid varchar(255), ccount INT, fcount INT, adm INT);")
dbSendQuery(conn, "CREATE TABLE zoo_users_class (zooid varchar(255), ccount varchar(255), pid varchar(255));")
dbSendQuery(conn, "CREATE TABLE zoo_users_favs (zooid varchar(255), fcount varchar(255), pid varchar(255));")

#PH talk (from mongo db)
mongo <- mongo.create(host="sociamvm-zooniverse.ecs.soton.ac.uk")

db <- "zooniverse"
coll <- "users"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  zooid <- ccount <- fcount <- adm <- NULL
  
  value <- mongo.cursor.value(curs)
    
  if(is.null(mongo.bson.value(value, 'zooniverse_id'))){
    zooid <- 0
  } else {
    zooid <- mongo.bson.value(value, 'zooniverse_id')
  }
  
  if(is.null(mongo.bson.value(value, 'classification_count'))){
    ccount <- 0
  } else {
    ccount <- mongo.bson.value(value, 'classification_count')
  }
  
  if(is.null(mongo.bson.value(value, 'favorite_count'))){
    fcount <- 0
  } else {
    fcount <- mongo.bson.value(value, 'favorite_count')
  }
  
  if(is.null(mongo.bson.value(value, 'admin'))){
    adm <- 0
  } else if(mongo.bson.value(value, 'admin')){
    adm <- 1
  } else{
    adm <- 0
  }
  
  # iterate project specific stats for user
  if(!is.null(mongo.bson.value(value, 'projects'))){
    
    projects <- mongo.bson.value(value,'projects')
    
    project_ids <- names(projects)
    
    for(prj in project_ids){
      pccount <- 0
      pfcount <- 0
      pid <- prj
      
      db2 <- "zooniverse"
      coll2 <- "projects"
      ns2 <- paste(db2, coll2, sep = ".")
      
      buf <- mongo.bson.buffer.create()
      mongo.bson.buffer.append(buf, "_id", mongo.oid.from.string(prj))
      query <- mongo.bson.from.buffer(buf)
      curs2 <- mongo.find.one(mongo, ns2, query)
      if(!is.null(curs2)){
        if(!is.null(mongo.bson.value(curs2, 'site_prefix'))){
          pid <- mongo.bson.value(curs2, 'site_prefix')
        }
      }
      
      pccount <- projects[prj][[1]]['classification_count']
      if(is.na(pccount) || is.null(pccount)){
        pccount <- 0
      }
      dbSendQuery(conn, paste("INSERT INTO zoo_users_class VALUES('", zooid,"','", pccount, "','", pid, "')",sep=""))
      pfcount <- projects[prj][[1]]['favorite_count']
      if(is.na(pfcount) || is.null(pfcount)){
        pfcount <- 0
      }
      dbSendQuery(conn, paste("INSERT INTO zoo_users_favs VALUES('", zooid,"','", pfcount, "','", pid, "')",sep=""))
    }    
    
  }
  
  #dbSendQuery(conn, paste("INSERT INTO zoo_users VALUES('", zooid,"','", ccount, "','", fcount, "','", adm, "')",sep=""))
}