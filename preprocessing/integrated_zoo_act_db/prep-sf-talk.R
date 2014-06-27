library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(RMySQL)
library(rjson)

options(error=recover)

#conn <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost/voc",uid="",pwd="")

conn <- dbConnect(dbDriver("MySQL"), user = "", password = "", dbname = "zoo_act")

#SF talk (from mongo db)
mongo <- mongo.create(host="<server>")

db <- "oceans_production"
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
    db2 <- "oceans_production"
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
      #print(paste("INSERT INTO zoo_act_references VALUES('", actid, "','", ref,"','Mention')",sep=""))
      dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", ref,"','Mention')",sep=""))
    }
  }
  
  if(length(mongo.bson.value(value, 'tags'))==0){
    ctags <- ""
  } else{
    for(tag in mongo.bson.value(value, 'tags')){
      #print(paste("INSERT INTO zoo_act_tags VALUES('", actid, "','", tag,"')",sep=""))
      dbSendQuery(conn, paste("INSERT INTO zoo_act_tags VALUES('", actid, "','", tag,"')",sep=""))
    }
  }
  
  if(is.null(mongo.bson.value(value, 'response_to_id'))){
    parentid <- 0
  } else {parentid <- mongo.oid.to.string(mongo.bson.value(value, 'response_to_id'))  }
  
  if(is.null(mongo.bson.value(value, 'discussion_id'))){
    discid <- 0
  } else {
    discid <- mongo.oid.to.string(mongo.bson.value(value, 'discussion_id'))  
    db2 <- "oceans_production"
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
          db3 <- "oceans_production"
          coll3 <- "assets"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            #print(paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Subject')",sep=""))
            dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Subject')",sep=""))
          }
        } else if(focus_type == "Group" ){
          db3 <- "oceans_production"
          coll3 <- "groups"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            #print(paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Group')",sep=""))
            dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Group')",sep=""))
          }
        } else if(focus_type == "Collection" ){
          db3 <- "oceans_production"
          coll3 <- "collections"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            #print(paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Collection')",sep=""))
            dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'zooniverse_id'),"','Collection')",sep=""))
          }
        } else if(focus_type == "Board" ){
          db3 <- "oceans_production"
          coll3 <- "boards"
          ns3 <- paste(db3, coll3, sep = ".")
          
          buf2 <- mongo.bson.buffer.create()
          mongo.bson.buffer.append(buf2, "_id", mongo.bson.value(curs2, 'focus_id'))
          query2 <- mongo.bson.from.buffer(buf2)
          curs3 <- mongo.find.one(mongo, ns3, query2)
          if(!is.null(curs3)){
            #print(paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'pretty_title'),"','Board')",sep=""))
            dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(curs3, 'pretty_title'),"','Board')",sep=""))
          }
        } else {}
      }
    }
  } 
  
  pid <- "SF"
  
  tut <- 0
  #print(paste("INSERT INTO zoo_act VALUES('", tstamp, "','", actid,"','", parentid,"','", discid,"','", zooid,"','", adm, "','", mod, "','", sci, "','", ctype,"','", cbody,"','", pid,"','", tut,"','", userid,"')",sep=""))
  dbSendQuery(conn, paste("INSERT INTO zoo_act VALUES('", tstamp, "','", actid,"','", parentid,"','", discid,"','", zooid,"','", adm, "','", mod, "','", sci, "','", ctype,"','", cbody,"','", pid,"','", tut,"','", userid,"')",sep=""))
  
}
