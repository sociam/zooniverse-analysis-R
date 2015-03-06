library(plyr)
library(RMySQL)
library(RJSONIO)
library(igraph)
library(tm)


ds <- list()
#projects = c('NN','CC','PH','SG','SW','GZ','WS','PF','SF','AP')
#projects = c('NN')
j<-'all'

#for(j in projects) {
  conn <- dbConnect(dbDriver("MySQL"), host="localhost", user = "root", password = "", dbname = "zoo_act")
  #ds[[j]] <- dbGetQuery(conn,paste("select act.actid, GROUP_CONCAT(tag.ctag SEPARATOR ', ') as tags, UNIX_TIMESTAMP(tstamp) from zoo_act_tags as tag INNER JOIN zoo_act as act ON act.actid=tag.actid WHERE act.pid='",j,"' group by act.actid;",sep=''))
  ds[[j]] <- dbGetQuery(conn,paste("select act.actid, GROUP_CONCAT(tag.ctag SEPARATOR ', ') as tags, UNIX_TIMESTAMP(tstamp) from zoo_act_tags as tag INNER JOIN zoo_act as act ON act.actid=tag.actid group by act.actid;",sep=''))
  
  nodes <- c()
  links <- c()
  roots <- c()
  
  last_node <- list()
  
  # WS, PH, GZ, PF, SF, SG, NN, CC, SW, AP
  ds2<-ds[[j]]
  
  for(i in 1:nrow(ds2)){
    tags<-unlist(strsplit(ds2[i,2],', '))
    #for(j in 1:length(tags)){
    #  tags[i] <- stemDocument(gsub("-", "",gsub("_", "", tags[i])))
    #}
    #ds2[i,2] <- paste(tags,collapse=', ')
    nodes <- rbind(nodes, c(ds2[i,1],ds2[i,2],ds2[i,3]))
    
    for(j in 1:length(tags)){
      #cur_tag <- stemDocument(gsub("-", "",gsub("_", "", tags[j])))
      cur_tag <- tags[j]
      if(!is.null(unlist(last_node[cur_tag]))){ #link back to last posts with this tag
        source_node <- last_node[cur_tag]
        target_node <- ds2[i,1]
        links <- rbind(links, c(source_node,target_node,roots[which(roots[,2]==cur_tag),1],cur_tag))
      } else {
        roots <- rbind(roots, c(ds2[i,1],cur_tag))
      }
      last_node[cur_tag] <- ds2[i,1]
    }
    
  }
  colnames(nodes) <- c('node_id','tags','dpub')
  colnames(links) <- c('source','target','root_node_id','tag')
  colnames(roots) <- c('root_node_id','tag')
  conn1 <- dbConnect(dbDriver("MySQL"), host="localhost", user = "root", password = "", dbname = "zoo_cascades")
  
  for(l in 1:nrow(nodes)){
    #print(paste('{"name":"',nodes[l,1],'","tags":"',gsub('-','',gsub('_','',ds2[which(ds2==nodes[l,1]),2])),'"},',sep=''))
    dbSendQuery(conn1, paste("INSERT INTO nodes (uri, dpub, interactions) VALUES('", nodes[l,1], "', UNIX_TIMESTAMP(", nodes[l,3] ,"),'", nodes[l,2] ,"')",sep=""))
  }
  
  for(k in 1:nrow(links)){
    dbSendQuery(conn1, paste("INSERT INTO links (source_node_uri, target_node_uri, interaction, root_node_uri) VALUES('", links[k,1], "','", links[k,2] ,"','", links[k,4] ,"','", links[k,3] ,"')",sep=""))
  }
  
  for(m in 1:nrow(roots)){
    dbSendQuery(conn1, paste("INSERT INTO roots (root_node_uri, interaction) VALUES('", roots[m,1], "','", roots[m,2] ,"')",sep=""))
  }
  
  edges <- links[,c(1,2)]
  colnames(edges) <- c("id1","id2")
  
  #generate the full graph
  g <- graph.data.frame(edges,directed=FALSE)
  
  clus <- clusters(g)$membership
  cascades <- list()
  conn1 <- dbConnect(dbDriver("MySQL"), host="localhost", user = "root", password = "", dbname = "zoo_cascades")
  for(z in 1:length(clus)){
    cascades[[clus[z]]] <- c(cascades[clus[z]][[1]],get.vertex.attribute(g,"name")[z])
  }
  conn1 <- dbConnect(dbDriver("MySQL"), host="localhost", user = "root", password = "", dbname = "zoo_cascades")
  for(w in 1:length(cascades)){
    nextcas <- dbGetQuery(conn1, paste("INSERT INTO cascades (path_cnt) VALUES(",length(cascades[[w]]),") RETURNING id",sep=""))
    for(v in 1:length(cascades[[w]])){
      dbSendQuery(conn1, paste("INSERT INTO nodes_cascades (node_uri, cascades_id) VALUES('", cascades[[w]][v] , "','",nextcas,"')",sep=""))
    }
  }
#}