# Project: Rango
# 
# Author: Willem Ligtenberg - willem.ligtenberg@openanalytics.eu
###############################################################################

#' Object for the connection to the database
#' @name RangoConnection-class
#' @rdname RangoConnection-class
#' @exportClass RangoConnection
#' @author Willem Ligtenberg
#' @export
setClass(
    Class = "RangoConnection",
    representation = representation(
        objectCache = "environment",
        cache = "logical"))

#' Create connection to data base
#' @param host host name
#' @param port port on which the database server is listening
#' @param dbname database name
#' @param user user
#' @param password password
#' @param type database type (PostgreSQL)
#' @param cache boolean that specifies if caching should be used
#' @return Connection object
#' @author Willem Ligtenberg
#' @export
#' @importFrom methods new
#' @importFrom logging basicConfig
#' @importFrom logging loglevels
rangoConnection <- function(dbname, host = NA, port = 5432,  
    user = NA, password = NA, type = "PostgreSQL", cache = FALSE, 
    logLevel = loglevels["INFO"]){
  basicConfig(logLevel)
  if(type == "PostgreSQL"){
    logdebug("Creating PostgreSQL connection")
    dbc <- new(Class = "RangoPostgresConnection", host = host, port = port,
        dbname = dbname, user = user, password = password, cache = cache)
    logdebug(str(dbc))
    return(dbc)
  }
  if(type == "SQLite"){
    logdebug("Creating SQLite connection")
    dbc <- new(Class = "RangoSQLiteConnection", dbname = dbname, cache = cache)
    logdebug(str(dbc))
    return(dbc)
  }
}

#' Reconnect to the database without losing the local cache
#' @param dbc Old RangoConnection object
#' 
#' @author Willem Ligtenberg
#' @export
setGeneric(
    name = "reconnect",
    def = function(dbc){standardGeneric("reconnect")})

#' Begin an new transaction
#' @param object the database connection to use
#' 
#' @author Willem Ligtenberg
#' @export
setGeneric(
    name = "beginTransaction",
    def = function(object){standardGeneric("beginTransaction")})

#' Commit the changes in the current transaction
#' @param object the database connection to use
#' 
#' @author Willem Ligtenberg
#' @export
#' @importFrom RPostgreSQL dbCommit
setGeneric(
    name = "commit",
    def = function(object){standardGeneric("commit")})

#' Roll back the changes in the current transaction
#' @param object the database connection to use
#' @author Willem Ligtenberg
#' @export
setGeneric(
    name = "rollback",
    def = function(object){standardGeneric("rollback")})