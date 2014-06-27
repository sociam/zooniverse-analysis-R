library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(RMySQL)
library(rjson)

#conn <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost/voc",uid="bokker",pwd="bokker")

conn <- dbConnect(dbDriver("MySQL"), user = "root", password = "", dbname = "zoo_act")

#create the activity db
# tstamp (timestamp of activity)
# actid (activity id)
# parentid (activity id of the parent activity if activity is a response)
# discid (discussion id of the thread if activity is a talk entry)
# zooid (zooniverse user id)
# adm (user role admin?)
# mod (user role moderator?)
# sci (user role scientist?)
# ctype (type of the activity [task|talk|forum])
# cref (objects the activity refers to, if any)
# cbody (content body of the activity, if any)
# ctags (tags of the activity, if any)
# pid (project id)
# tut (performed while in tutorial mode?)
#dbSendQuery(conn, "CREATE TABLE zoo_act (tstamp varchar(255), actid varchar(255), parentid varchar(255), discid varchar(255), zooid BIGINT, adm INT, mod INT, sci INT, ctype varchar(255), cbody CLOB, pid varchar(255), tut INT, userid varchar(255));")
#dbSendQuery(conn, "CREATE TABLE zoo_act_references (actid varchar(255), cref varchar(255), reftype varchar(255));")
#dbSendQuery(conn, "CREATE TABLE zoo_act_tags (actid varchar(255), ctag varchar(255));")
#dbSendQuery(conn, "CREATE TABLE zoo_act(tstamp VARCHAR(255), actid VARCHAR(255), parentid VARCHAR(255), discid VARCHAR(255), zooid INTEGER, radmi INTEGER, rmod INTEGER, rsci INTEGER, ctype VARCHAR(255), cbody TEXT, pid VARCHAR(255), tut INTEGER, fullid VARCHAR(255))")
#dbSendQuery(conn, "CREATE TABLE zoo_act_references(actid VARCHAR(255), cref VARCHAR(255), reftype VARCHAR(255))")
#dbSendQuery(conn, "CREATE TABLE zoo_act_tags(actid VARCHAR(255), ctag VARCHAR(255))")


#ds1 <- dbGetQuery(conn,"select a.actid from zoo_act as a where a.ctype = 'talk' AND pid ='PH';")

#for(atc in ds1){
#  tmp <- dbGetQuery(conn,paste("delete from zoo_act_references WHERE actid ='",atc,"';",sep=""))
#}

#for(atc in ds1){
#  tmp <- dbGetQuery(conn,paste("delete from zoo_act_tags WHERE actid ='",atc,"';",sep=""))
#}

#tmp <- dbGetQuery(conn,"delete from zoo_act_references WHERE length(actid)<2;")
#tmp <- dbGetQuery(conn,"delete from zoo_act where ctype = 'talk' AND pid ='PH';")

#PH talk (from mongo db)
mongo <- mongo.create(host="sociamvm-zooniverse.ecs.soton.ac.uk")

db <- "sellers"
coll <- "comments"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  tstamp <- actid <- parentid <- discid <- zooid <- userid <- adm <- mod <- sci <- ctype <- cref <- cbody <- ctags <- pid <- tut <- NULL
  
  value <- mongo.cursor.value(curs)
  
  userid <- 0
  
  if(is.null(mongo.bson.value(value, 'author_id'))){
    zooid <- 0
  } else {
    author_id <- mongo.bson.value(value, 'author_id')
    userid <- mongo.oid.to.string(mongo.bson.value(value, 'author_id'))
    db2 <- "sellers"
    coll2 <- "users"
    ns2 <- paste(db2, coll2, sep = ".")
    
    buf <- mongo.bson.buffer.create()
    mongo.bson.buffer.append(buf, "_id", author_id)
    query <- mongo.bson.from.buffer(buf)
    curs2 <- mongo.find.one(mongo, ns2, query)
    if(!is.null(curs2)){
      zooid <- mongo.bson.value(curs2, 'zooniverse_user_id')
      
      if(is.null(mongo.bson.value(curs2, 'admin')) || !mongo.bson.value(curs2, 'admin')){
        adm <- 0
      } else{
        adm <- 1
      }
      
      if(is.null(mongo.bson.value(curs2, 'moderator')) || !mongo.bson.value(curs2, 'moderator')){
        mod <- 0
      } else{
        mod <- 1
      }
      
      if(is.null(mongo.bson.value(curs2, 'scientist')) || !mongo.bson.value(curs2, 'scientist')){
        sci <- 0
      } else{
        sci <- 1
      }
    }
  }
  
  ctype <- "talk"
  
  if(is.null(mongo.bson.value(value, 'body'))){
    cbody <- ""
  } else{cbody <- gsub('\'|\\\\|/|\"', '_', iconv(mongo.bson.value(value, 'body'),  "latin1", "ASCII", "?"))}
  
  if(is.null(mongo.bson.value(value, 'created_at'))){
    tstamp <- "na"
  } else{tstamp <- toString(mongo.bson.value(value, 'created_at')) }
  
  if(is.null(mongo.bson.value(value, '_id'))){
    actid <- 0
  } else {actid <- mongo.oid.to.string(mongo.bson.value(value, '_id'))  }
  
  if(length(mongo.bson.value(value, 'mentions'))==0){
    cref <- ""
  } else{
    for(ref in mongo.bson.value(value, 'mentions')){
      tryCatch({dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", ref,"','Mention')",sep=""))}, error=function(e) NULL, finally = print("skipped"))
    }
  }
  
  if(length(mongo.bson.value(value, 'tags'))==0){
    ctags <- ""
  } else{
    for(tag in mongo.bson.value(value, 'tags')){
      tryCatch({dbSendQuery(conn, paste("INSERT INTO zoo_act_tags VALUES('", actid, "','", tag,"')",sep=""))}, error=function(e) NULL, finally = print("skipped"))
    }
  }
  
  if(is.null(mongo.bson.value(value, 'response_to_id'))){
    parentid <- 0
  } else {parentid <- mongo.oid.to.string(mongo.bson.value(value, 'response_to_id'))  }
  
  if(is.null(mongo.bson.value(value, 'discussion_id'))){
    discid <- 0
  } else {
    discid <- mongo.oid.to.string(mongo.bson.value(value, 'discussion_id'))  
    db2 <- "sellers"
    coll2 <- "discussions"
    ns2 <- paste(db2, coll2, sep = ".")
    
    buf <- mongo.bson.buffer.create()
    mongo.bson.buffer.append(buf, "_id", mongo.bson.value(value, 'discussion_id'))
    query <- mongo.bson.from.buffer(buf)
    curs2 <- mongo.find.one(mongo, ns2, query)
    if(!is.null(curs2)){
      if(!is.null(mongo.bson.value(curs2, 'focus_type'))){
        focus_type = mongo.bson.value(curs2, 'focus_type')
        if(focus_type == "Asset" ){
          db3 <- "sellers"
          coll3 <- "assets"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            tryCatch({dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Subject')",sep=""))}, error=function(e) NULL, finally = print("skipped"))
          }
        } else if(focus_type == "Group" ){
          db3 <- "sellers"
          coll3 <- "groups"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            tryCatch({dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Group')",sep=""))}, error=function(e) NULL, finally = print("skipped"))
          }
        } else if(focus_type == "Collection" ){
          db3 <- "sellers"
          coll3 <- "collections"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            tryCatch({dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Collection')",sep=""))}, error=function(e) NULL, finally = print("skipped"))
          }
        } else if(focus_type == "Board" ){
          db3 <- "sellers"
          coll3 <- "boards"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            tryCatch({dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'pretty_title'),"','Board')",sep=""))}, error=function(e) NULL, finally = print("skipped"))
          }
        } else {}
      }
    }
  } 
  
  pid <- "PH"
  
  tut <- 0
  
  tryCatch({dbSendQuery(conn, paste("INSERT INTO zoo_act VALUES('", tstamp, "','", actid,"','", parentid,"','", discid,"','", zooid,"','", adm, "','", mod, "','", sci, "','", ctype,"','", cbody,"','", pid,"','", tut,"','", userid,"')",sep=""))}, error=function(e) NULL, finally = print("skipped"))
  
}
