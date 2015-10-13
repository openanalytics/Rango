# Project: Rango
# 
# Author: Willem Ligtenberg - willem.ligtenberg@openanalytics.eu
###############################################################################


#' Object for the connection to the Postgres database
#' @name RangoPostgresConnection-class
#' @rdname RangoPostgresConnection-class
#' @exportClass RangoPostgresConnection
#' @author Willem Ligtenberg
#' @export
#' @include RangoConnection.R
#' @include utils.R
#' @importClassesFrom RPostgreSQL PostgreSQLConnection
setClass(
    Class = "RangoPostgresConnection",
    representation = representation(
        host = "character",
        dbname = "character",
        port = "numeric",
        user = "character",
        password = "character",
        con = "PostgreSQLConnection"),
    contains = "RangoConnection")

#' Initialize the RangoPostgresConnection class
#' @name initialize
#' @aliases \S4method{initialize}{RangoPostgresConnection}
#' 
#' @param .Object RangoPostgresConnection object
#' @param host host name
#' @param dbname database name
#' @param port port on which the database server is listening
#' @param user user
#' @param password password
#' @param cache boolean which specifies if we use a local cache
#' @return RangoConnection object
#' @author Willem Ligtenberg
#' @export
#' @importFrom DBI dbDriver
#' @importFrom RPostgreSQL dbConnect
setMethod(
    f = "initialize",
    signature = "RangoPostgresConnection",
    definition = function(.Object, host, dbname, port, user, password, cache){
      drv <- dbDriver("PostgreSQL", max.con = 25)
      .Object@host <- host
      .Object@dbname <- dbname
      .Object@port <- port
      .Object@user <- user
      .Object@password <- password
      .Object@con <- dbConnect(drv, host = host, dbname = dbname, port = port,
          user = user, password = password)
      .Object@objectCache <- new.env(parent = .GlobalEnv)
      .Object@cache <- cache
      return(.Object)
    })

#' Reconnect to the database without losing the local cache
#' @param dbc Old RangoPostgresConnection object
#' @return renewed RangoPostgresConnection object
#' @author Willem Ligtenberg
#' @export
#' @importFrom RPostgreSQL dbDisconnect
setMethod(
    f = "reconnect",
    signature = "RangoPostgresConnection",
    definition = function(dbc){
      dbc2 <- dbc
      dbDisconnect(dbc@con)
      dbc <- rangoConnection(host = dbc@host, port = dbc@port,
          dbname = dbc@dbname, user = dbc@user, password = dbc@password, 
          type = "PostgreSQL", cache = dbc@cache)
      dbc@objectCache <- dbc2@objectCache
      return(dbc)
    })

#' Begin an new transaction
#' @param object the database connection to use
#' 
#' @author Willem Ligtenberg
#' @export
#' @importFrom RPostgreSQL dbGetQuery
setMethod(
    f = "beginTransaction",
    signature = "RangoPostgresConnection",
    definition = function(object){
      dbGetQuery(object@con, "BEGIN TRANSACTION")
    })

#' Commit the changes in the current transaction
#' @param object the database connection to use
#' 
#' @author Willem Ligtenberg
#' @export
#' @importFrom RPostgreSQL dbCommit
setMethod(
    f = "commit",
    signature = "RangoPostgresConnection",
    definition = function(object){
      dbCommit(object@con)
    })

#' Roll back the changes in the current transaction
#' @author Willem Ligtenberg
#' @importFrom RPostgreSQL dbRollback
setMethod(
    f = "rollback",
    signature = "RangoPostgresConnection",
    definition = function(object){
      dbRollback(object@con)
    })

#' list the tables for the current data base 
#' @return vector containing table names in the data base
#' @author Willem Ligtenberg
#' @importFrom RPostgreSQL dbListTables
setMethod(
    f = "listTables",
    signature = "RangoPostgresConnection",
    definition = function(dbc){
      query <- "RANGO_dbListTables"
      result <- NA 
      if(is.null(dbc@objectCache[[query]])){
        result <- dbListTables(dbc@con)
        assign(query, result, dbc@objectCache)
      } else{
        result <- dbc@objectCache[[query]]
      }
      return(result)
    })

#' generate S4 classes and functions for a specific table in the data base
#' @param dbc RangoPostgresConnection
#' @param tableName name of the table
#' @return string containing the class definition
#' @author Willem Ligtenberg
#' @importFrom plyr ddply
#' @importFrom Hmisc capitalize
#' @importFrom RPostgreSQL dbGetQuery
setMethod(
    f = "generateClass",
    signature = "RangoPostgresConnection",
    definition = function(dbc, tableName){
      logdebug(tableName)
      query <- paste0("SELECT  
              f.attnum AS number,  
              f.attname AS name,  
              f.attnotnull AS notnull,  
              pg_catalog.format_type(f.atttypid,f.atttypmod) AS type,  
              CASE
              WHEN p.contype = 'f' THEN g.relname
              END AS foreignkey,
              CASE
              WHEN f.atthasdef = 't' THEN d.adsrc
              END AS default,
              CASE  
              WHEN p.contype = 'u' THEN 't'  
              ELSE 'f'
              END AS uniquekey
              FROM pg_attribute f  
              JOIN pg_class c ON c.oid = f.attrelid
              JOIN pg_type t ON t.oid = f.atttypid    
              LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = f.attnum  
              LEFT JOIN pg_namespace n ON n.oid = c.relnamespace  
              LEFT JOIN pg_constraint p ON p.conrelid = c.oid AND 
              f.attnum = ANY ( p.conkey ) 
              LEFT JOIN pg_class AS g ON p.confrelid = g.oid  
              WHERE c.relkind = 'r'::char  
              AND n.nspname = 'public'  
              AND c.relname = '", tableName, "'  
              AND f.attnum > 0 ORDER BY number;")
      
      columnData <- unique(dbGetQuery(dbc@con, statement = query))
      
      columnData <- ddply(columnData, "number", function(x) {
            x[which.min(apply(x, 1, function(y) sum(is.na(y)))), ]
          })
      
      className <- capitalize(tableName)
      
      # Create S4 class
      
      classDefinition <- c("setClass(",
          paste0("\tClass = '", className, "',"),
          "\tslots = list(\n\t\trangoBookKeeping = 'list',")
      
      for(i in seq_len(nrow(columnData) - 1)){
        classDefinition <- c(classDefinition, 
            paste0("\t\t", columnData[i, "name"], " = '", 
                dataType(columnData[i, ]), "',"))
      }
      
      classDefinition <- c(classDefinition, 
          paste0("\t\t", columnData[nrow(columnData), "name"], " = '", 
              dataType(columnData[nrow(columnData), ]), 
              "'),\n\tcontains = 'RangoObject')", "\n"))
      
      # Add S3 method
      arguments <- paste(paste0(columnData$name, " = NULL"), collapse = ", ")
      classDefinition <- c(classDefinition,
          paste0(tableName, " <- function(", arguments, "){"),
          paste0("\trangoLength <- max(sapply(list(", 
              paste(columnData$name, collapse = ", "), "), length))"),
          paste0("\ttmp <- vector('list', rangoLength)"),
          paste0("\tfor(i in seq_len(rangoLength)){"),
          paste0("\t\ttmp[[i]] <- new(Class = '", className, "')\n", 
              paste(sapply(columnData$name, function(x) 
                        paste0("\t\tif(!missing(", x, ")){if(is.list(", x, 
                            ")){tmp[[i]]@", x, " <- ", x, 
                            "[[i]]} else {if(inherits(", x, 
                            ", 'RangoArgument')){eval(substitute(tmp[[i]]@n <- ", 
                            x, ", list(n = names(", x, "))))} else {tmp[[i]]@", 
                            x, " <- ", x, "}}}")), 
                  collapse = "\n"), 
              "\n\t\ttmp[[i]]@rangoBookKeeping[['retrieved']] <- FALSE",
              "\n\t}",
              "\n\tif(rangoLength == 1){\n", 
              "\t\ttmp <- tmp[[1]]\n",
              "\t}\n",
              "\treturn(tmp)\n}\n", "\n")
      )
      logdebug(classDefinition)
      return(classDefinition)
    })
    
    
#' Get the primary key for a table
#' @param tableName name of the table
#' @param dbc database connection to use
#' @return string that contains the primary key of the table
#' @author Willem Ligtenberg
#' @export
#' @importFrom RPostgreSQL dbGetQuery
setMethod(
    f = "getPrimaryKey",
    signature = "RangoPostgresConnection",
    definition = function(dbc, tableName){
      query <- paste0("SELECT c.column_name ",
          "FROM information_schema.table_constraints tc ",
          "JOIN information_schema.constraint_column_usage AS ccu ",
          "USING (constraint_schema, constraint_name) ",
          "JOIN information_schema.columns AS c ",
          "ON c.table_schema = tc.constraint_schema AND ",
          "tc.table_name = c.table_name AND ccu.column_name = c.column_name ",
          "WHERE constraint_type = 'PRIMARY KEY' AND ",
          "tc.table_name = '", tableName, "';")
      result <- NA 
      if(is.null(dbc@objectCache[[query]])){
        result <- dbGetQuery(dbc@con, statement = query)$column_name
        # Save the object in the environment
        if(dbc@cache){
          assign(query, result, dbc@objectCache)
        }
      } else{
        result <- dbc@objectCache[[query]]
      }
      return(result)
    })

#' store the object to the data base
#' @return returns (a list of) object with all the 
#' information from the data base (or a data.frame if chosen)
#' @author Willem Ligtenberg
#' @importFrom RPostgreSQL dbSendQuery
setMethod(
    f = "store",
    signature = signature(object = "RangoObject", dbc = "RangoPostgresConnection"),
    definition = function(object, dbc, force = FALSE, returnType = "RangoObject"){
      if(suppressWarnings(
          is.na(retrieve(object, dbc, create = FALSE))) | force){
        # save it to the db
        query <- paste0("INSERT INTO ", tolower(class(object)), 
            " (", paste(nonEmptySlots(object, dbc), collapse = ", "), 
            ") VALUES (", 
            paste(sapply(nonEmptySlots(object, dbc), function(x) 
                      formatter(eval(
                              parse(text = paste0("object@", x))), dbc)),
                collapse = ", "), ") RETURNING *")
        logdebug(query)
        res <- dbGetQuery(dbc@con, statement = query)
        retVal <- switch(returnType,
            RangoObject = createObjectFromResult(object, dbc, res, create),
            data.frame = res,
            None = NULL)
        return(retVal)
      }
    })

#' store the list to the data base
#' @return returns (a list of) object with all the 
#' information from the data base (or a data.frame if chosen)
#' @author Willem Ligtenberg
#' @importFrom RPostgreSQL dbSendQuery
setMethod(
    f = "store",
    signature = signature(object = "list", dbc = "RangoPostgresConnection"),
    definition = function(object, dbc, force = FALSE, returnType = "RangoObject"){
      suppressWarnings(
          if(is.na(retrieve(object, dbc, create = FALSE)) | force){
            # save it to the db
            columnNames <- unique(lapply(object, nonEmptySlots, dbc))
            if(length(columnNames) == 1){
              columnNames <- columnNames[[1]]
            }else {
              stop("We currently only support storing list of objects that have the same dimensions")
            }
            
            vals <- paste(sapply(object, function(obj, columnNames) {
                      paste("(", paste(sapply(columnNames, function(x) {
                                    formatter(eval(parse(text = paste0("obj@", x))), dbc)
                                  }), collapse = ", "), ")", 
                          sep = "")}, columnNames), collapse = ", ")
            
            query <- paste0("INSERT INTO ", tolower(class(object[[1]])), 
                " (", paste(columnNames, collapse = ", "), 
                ") VALUES ", vals, " RETURNING *")  
            logdebug(query)
            res <- dbGetQuery(dbc@con, statement = query)
            retVal <- switch(returnType,
                RangoObject = createObjectFromResult(object, dbc, res, create),
                data.frame = res,
                None = NULL)
            return(retVal)
          })
    })