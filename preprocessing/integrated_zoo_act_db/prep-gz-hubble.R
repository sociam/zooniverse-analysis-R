library(RMySQL)
library(MonetDB.R)
library(rjson)

conn <- dbConnect(dbDriver("MonetDB"), "monetdb://localhost/voc",uid="bokker",pwd="bokker")

# connect to mysql server
mysql <- dbConnect(dbDriver("MySQL"), dbname="galaxy_zoo_forum",user="root")

