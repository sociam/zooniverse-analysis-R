library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(RMySQL)
library(rjson)

#conn <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost/voc",uid="",pwd="")
conn <- dbConnect(dbDriver("MySQL"), user = "", password = "", dbname = "zoo_act")

#ds1 <- dbGetQuery(conn,"select a.actid from zoo_act as a where a.ctype = 'task';")

#for(atc in ds1){
#  tmp <- dbGetQuery(conn,paste("delete from zoo_act_references WHERE actid ='",atc,"'",sep=""))
#}

#tmp <- dbGetQuery(conn,"delete from zoo_act WHERE ctype ='task'")

mongo <- mongo.create(host="<server>")


# proceed with classification data for all projects

projects <- c('andromeda_classifications','bat_detective_classifications','cancer_cells_classifications','cancer_gene_runner_classifications','cyclone_center_classifications','galaxy_zoo_classifications','galaxy_zoo_quiz_classifications','leaf_classifications','notes_from_nature_classifications','planet_four_classifications','plankton_classifications','radio_classifications','sea_floor_classifications','serengeti_classifications','spacewarp_classifications','worms_classifications')

db <- "zooniverse"

for(p in projects){
  if (mongo.is.connected(mongo))
    mongo.reconnect(mongo)
  # move to next project collection  
  coll <- p
  ns <- paste(db, coll, sep = ".")
  
  curs <- mongo.find(mongo, ns)
  
  while (mongo.cursor.next(curs)) {
    if (mongo.is.connected(mongo))
      mongo.reconnect(mongo)
    
    tstamp <- actid <- parentid <- zooid <- userid <- adm <- mod <- sci <- ctype <- cref <- cbody <- ctags <- pid <- tut <- NULL
    
    value2 <- mongo.cursor.value(curs)
    project_id <- mongo.bson.value(value2, 'project_id')
    discid <- 0
    userid <- 0
    
    if(is.null(mongo.bson.value(value2, 'tutorial'))) {
      tut <- 0
    } else if(mongo.bson.value(value2, 'tutorial')){
      tut <- 1
    } else{tut <- 0}
    
    ctype <- "task"
    
    if(is.null(mongo.bson.value(value2, '_id'))){
      actid <- 0
    } else {actid <- mongo.oid.to.string(mongo.bson.value(value2, '_id'))  }
    #exists <- 0
    #exists <- dbGetQuery(conn,paste("select count(*) from zoo_act where actid = '",actid,"';",sep=""))
    #if(exists == 0){
      parentid <- 0
      cbody <- ""
      ctags <- ""
      
      if(is.null(mongo.bson.value(value2, 'created_at'))){
        tstamp <- "na"
      } else{tstamp <- toString(mongo.bson.value(value2, 'created_at')) }
      
      if(is.null(mongo.bson.value(value2, 'user_id'))){
        zooid <- 0
        adm <- 0
        mod <- 0
        sci <- 0
      } else {
        author_id <- mongo.bson.value(value2, 'user_id')
        userid <- mongo.oid.to.string(mongo.bson.value(value2, 'user_id'))
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
        } else{
          zooid <- 0
        }
      }
      
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
      
      subjects <- mongo.bson.value(mongo.cursor.value(curs),'subjects')
        
      for(i in subjects){
        value <- mongo.bson.from.list(i)
        dbSendQuery(conn, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", mongo.bson.value(value, 'zooniverse_id'),"','Subject')",sep=""))
      }
      
      dbSendQuery(conn, paste("INSERT INTO zoo_act VALUES('", tstamp, "','", actid,"','", parentid,"','", discid,"','", zooid,"','", adm, "','", mod, "','", sci, "','", ctype,"','", cbody,"','", pid,"','", tut,"','", userid,"')",sep=""))
    #}
  }
}