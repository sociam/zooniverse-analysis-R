library(rmongodb)
library(googleVis)
library(zoo)
library(MonetDB.R)
library(RMySQL)
library(rjson)

#conn <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost/voc",uid="",pwd="")

conn <- dbConnect(dbDriver("MySQL"), user = "", password = "", dbname = "planet_production")
conn2 <- dbConnect(dbDriver("MySQL"), user = "", password = "", dbname = "zoo_act")
mongo <- mongo.create(host="<server>")
curs <- dbGetQuery(conn,"select c.id as actid, c.zooniverse_user_id as author_id, l.zooniverse_id as mentions, c.created_at as created_at from classifications as c INNER JOIN light_curves as l ON c.light_curve_id=l.id")

f <- function(x){
  tstamp <- actid <- parentid <- discid <- zooid <- userid <- adm <- mod <- sci <- ctype <- cref <- cbody <- ctags <- pid <- tut <- NULL
  
  userid <- 0
  discid <- 0
  parentid <- 0
  adm <- 0
  mod <- 0
  sci <- 0
  
  if(!is.null(x['author_id'])){
    zooid <- x['author_id']
    
    db2 <- "sellers"
    coll2 <- "users"
    ns2 <- paste(db2, coll2, sep = ".")
    
    buf <- mongo.bson.buffer.create()
    mongo.bson.buffer.append(buf, "zooniverse_user_id", zooid)
    query <- mongo.bson.from.buffer(buf)
    curs2 <- mongo.find.one(mongo, ns2, query)
    if(!is.null(curs2)){
      
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
  
  ctype <- "task"
  cbody <- ""
  
  if(is.null(x['created_at'])){
    tstamp <- "na"
  } else{tstamp <- toString(x['created_at']) }
  
  if(is.null(x['actid'])){
    actid <- 0
  } else {actid <- paste("PHTASK",x['actid'],sep="")  }
  
  if(!is.null(x['mentions'])){
    dbSendQuery(conn2, paste("INSERT INTO zoo_act_references VALUES('", actid, "','", x['mentions'],"','Subject')",sep=""))
  }
  
  pid <- "PH"
  tut <- 0
  
  dbSendQuery(conn2, paste("INSERT INTO zoo_act VALUES('", tstamp, "','", actid,"','", parentid,"','", discid,"','", zooid,"','", adm, "','", mod, "','", sci, "','", ctype,"','", cbody,"','", pid,"','", tut,"','", userid,"')",sep=""))
  
}

apply(curs, 1, f)