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
#dbSendQuery(conn, "CREATE TABLE zoo_act (tstamp varchar(255), actid varchar(255), parentid varchar(255), discid varchar(255), zooid BIGINT, adm INT, mod INT, sci INT, ctype varchar(255), cbody CLOB, pid varchar(255), tut INT);")
#dbSendQuery(conn, "CREATE TABLE zoo_act_references (actid varchar(255), cref varchar(255), reftype varchar(255));")
#dbSendQuery(conn, "CREATE TABLE zoo_act_tags (actid varchar(255), ctag varchar(255));")

#PH talk (from mongo db)
mongo <- mongo.create(host="sociamvm-zooniverse.ecs.soton.ac.uk")

# classification and talk data for (all from mongo db)
# andromeda
# bat_detective
# cancer_cells (cell slider)
# cyclone_center
# galaxy_zoo_starburst
# galaxy_zoo (which version does this mean???)
# notes_from_nature
# planet_four
# plankton
# sea_floor
# serengeti
# spacewarp
# worms

# begin with talk for all orboros projects

db <- "zooniverse"
coll <- "discussions"
ns <- paste(db, coll, sep = ".")

curs <- mongo.find(mongo, ns)

while (mongo.cursor.next(curs)) {
  if (mongo.is.connected(mongo))
    mongo.reconnect(mongo)
  
  tstamp <- actid <- parentid <- zooid <- userid <- adm <- mod <- sci <- ctype <- cref <- cbody <- ctags <- pid <- tut <- NULL
  
  value2 <- mongo.cursor.value(curs)
  
  project_id <- mongo.bson.value(value2, 'project_id')
  
  discid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))
  userid <- 0
  
  db2 <- "zooniverse"
  coll2 <- "projects"
  ns2 <- paste(db2, coll2, sep = ".")
  
  buf <- mongo.bson.buffer.create()
  mongo.bson.buffer.append(buf, "_id", project_id)
  query <- mongo.bson.from.buffer(buf)
  curs2 <- mongo.find.one(mongo, ns2, query)
  if(!is.null(curs2)){
    pid <- mongo.bson.value(curs2, 'site_prefix')
  } else{
    pid <- mongo.oid.to.string(project_id)
  }
  
  tut <- 0
  
  ctype <- "talk"
  
  comments <- mongo.bson.value(mongo.cursor.value(curs),'comments')
  
  for(i in comments){
    value <- mongo.bson.from.list(i)
    
    if(is.null(mongo.bson.value(value, 'created_at'))){
      tstamp <- "na"
    } else{
      tstamp <- toString(mongo.bson.value(value, 'created_at'))
    }
    
    if(is.null(mongo.bson.value(value, '_id'))){
      actid <- 0
    } else {
      actid <- mongo.oid.to.string(mongo.bson.value(value, '_id'))  
    }
    
    if(is.null(mongo.bson.value(value, 'response_to_id'))){
      parentid <- 0
    } else {
      parentid <- mongo.oid.to.string(mongo.bson.value(value, 'response_to_id'))
    } 
    
    if(length(mongo.bson.value(value, 'mentions'))==0){
      cref <- ""
    } else{
      for(ref in mongo.bson.value(value, 'mentions')){
        dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", ref,"','Mention')",sep=""))
      }
    }
    
    if(!is.null(mongo.bson.value(value2, 'focus.base_type'))){
      dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(value2, 'focus._id'),"','", mongo.bson.value(value2, 'focus.base_type'),"')",sep=""))
    }
    
    if(is.null(mongo.bson.value(value, 'body'))){
      cbody <- ""
    } else{cbody <- gsub('\'|\\\\|/|\"', '_', iconv(mongo.bson.value(value, 'body'),  "latin1", "ASCII", "?"))}
    
    if(length(mongo.bson.value(value, 'tags'))==0){
      ctags <- ""
    } else{
      for(tag in mongo.bson.value(value, 'tags')){
        dbSendQuery(conn, paste("INSERT INTO zoo_act_tags VALUES('", actid, "','", tag,"')",sep=""))
      }
    }
    
    if(is.null(mongo.bson.value(value, 'user_id'))){
      zooid <- 0
      adm <- 0
      mod <- 0
      sci <- 0
    } else {
      author_id <- mongo.bson.value(value, 'user_id')
      userid <- mongo.oid.to.string(mongo.bson.value(value, 'user_id'))
      db2 <- "zooniverse"
      coll2 <- "users"
      ns2 <- paste(db2, coll2, sep = ".")
      
      buf <- mongo.bson.buffer.create()
      mongo.bson.buffer.append(buf, "_id", author_id)
      query <- mongo.bson.from.buffer(buf)
      curs2 <- mongo.find.one(mongo, ns2, query)
      if(!is.null(curs2)){
        zooid <- mongo.bson.value(curs2, 'zooniverse_id')
        
        if(is.null(mongo.bson.value(curs2, 'talk.roles'))){
          adm <- 0
          mod <- 0
          sci <- 0
        } else{
          talkroles <- mongo.bson.value(curs2,'talk.roles')
          
          if(talkroles[mongo.oid.to.string(project_id)] == 'admin') adm <- 1
          else adm <- 0
          
          if(talkroles[mongo.oid.to.string(project_id)] == 'moderator') mod <- 1
          else mod <- 0
          
          if(talkroles[mongo.oid.to.string(project_id)] == 'scientist') sci <- 1
          else sci <- 0
        }
      }
    }
    dbSendQuery(conn, paste("INSERT INTO zoo_act VALUES('", tstamp, "','", actid,"','", parentid,"','", discid,"','", zooid,"','", adm, "','", mod, "','", sci, "','", ctype,"','", cbody,"','", pid,"','", tut,"','", userid,"')",sep=""))
  }
}