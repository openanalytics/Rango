# Project: Rango
# 
# Author: Willem Ligtenberg - willem.ligtenberg@openanalytics.eu
###############################################################################


#' Object for the connection to the SQLite database
#' @name RangoSQLiteConnection-class
#' @rdname RangoSQLiteConnection-class
#' @exportClass RangoSQLiteConnection
#' @author Willem Ligtenberg
#' @export
#' @include RangoConnection.R
#' @include utils.R
#' @importClassesFrom RSQLite SQLiteConnection
setClass(
    Class = "RangoSQLiteConnection",
    representation = representation(
        dbname = "character",
        con = "SQLiteConnection"),
    contains = "RangoConnection")

#' Initialize the RangoSQLiteConnection class
#' @name initialize
#' @aliases \S4method{initialize}{RangoSQLiteConnection}
#' 
#' @param .Object RangoSQLiteConnection object
#' @param dbname database name
#' @param cache boolean which specifies if we use a local cache
#' @return RangoConnection object
#' @author Willem Ligtenberg
#' @export
#' @importFrom DBI dbDriver
#' @importFrom RSQLite dbConnect
setMethod(
    f = "initialize",
    signature = "RangoSQLiteConnection",
    definition = function(.Object, dbname, cache){
      drv <- dbDriver("SQLite")
      .Object@dbname <- dbname
      .Object@con <- dbConnect(drv, dbname = dbname)
      .Object@objectCache <- new.env(parent = .GlobalEnv)
      .Object@cache <- cache
      return(.Object)
    })

#' Reconnect to the database without losing the local cache
#' @param dbc Old RangoSQLiteConnection object
#' @return renewed RangoSQLiteConnection object
#' @author Willem Ligtenberg
#' @export
#' @importFrom RSQLite dbDisconnect
setMethod(
    f = "reconnect",
    signature = "RangoSQLiteConnection",
    definition = function(dbc){
      dbc2 <- dbc
      dbDisconnect(dbc@con)
      dbc <- rangoConnection(dbname = dbc@dbname, type = "SQLite", 
          cache = dbc@cache)
      dbc@objectCache <- dbc2@objectCache
      return(dbc)
    })

#' Begin an new transaction
#' @param object the database connection to use
#' 
#' @author Willem Ligtenberg
#' @export
#' @importFrom RSQLite dbBeginTransaction
setMethod(
    f = "beginTransaction",
    signature = "RangoSQLiteConnection",
    definition = function(object){
      dbBegin(object@con)
    })

#' Commit the changes in the current transaction
#' @param object the database connection to use
#' 
#' @author Willem Ligtenberg
#' @export
#' @importFrom RSQLite dbCommit
setMethod(
    f = "commit",
    signature = "RangoSQLiteConnection",
    definition = function(object){
      dbCommit(object@con)
    })

#' Roll back the changes in the current transaction
#' @author Willem Ligtenberg
#' @importFrom RSQLite dbRollback
setMethod(
    f = "rollback",
    signature = "RangoSQLiteConnection",
    definition = function(object){
      dbRollback(object@con)
    })

#' list the tables for the current data base 
#' @return vector containing table names in the data base
#' @author Willem Ligtenberg
#' @importFrom RSQLite dbListTables
setMethod(
    f = "listTables",
    signature = "RangoSQLiteConnection",
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
#' @importFrom Hmisc capitalize
setMethod(
    f = "generateClass",
    signature = "RangoSQLiteConnection",
    definition = function(dbc, tableName){
      logdebug(tableName)
      columnData <- dbGetQuery(dbc@con, statement = 
              paste0("PRAGMA table_info(", tableName, ");"))
      columnData$type <- tolower(columnData$type)
      fks <- dbGetQuery(dbc@con, statement = 
              paste0("PRAGMA foreign_key_list(", tableName, ");"))
      if(is.null(fks)){
        columnData$foreignkey <- NA
      } else{
        fks <- fks[, c("from", "table")]
        colnames(fks) <- c("from", "foreignkey")
        columnData <- merge(columnData, fks, by.x = "name", by.y = "from", 
            all.x = TRUE)
      }
#      indices <- dbGetQuery(dbc@con, statement = 
#              paste0("PRAGMA index_list(", tableName, ");"))
      
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
#' @importFrom RSQLite dbGetQuery
    setMethod(
        f = "getPrimaryKey",
        signature = "RangoSQLiteConnection",
        definition = function(dbc, tableName){
          query <- paste0("PRAGMA table_info(", tableName, ");")
          result <- NA 
          if(is.null(dbc@objectCache[[query]])){
            columnData <- dbGetQuery(dbc@con, statement = query)
            result <- columnData[columnData$pk == 1, "name"]
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
    signature = signature(object = "RangoObject", dbc = "RangoSQLiteConnection"),
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
                collapse = ", "), ")")
        logdebug(query)
        res <- dbGetQuery(dbc@con, statement = query)
        retVal <- switch(returnType,
            RangoObject = retrieve(object, dbc, create = FALSE),
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
    signature = signature(object = "list", dbc = "RangoSQLiteConnection"),
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
                ") VALUES ", vals, "")  
            logdebug(query)
            res <- dbGetQuery(dbc@con, statement = query)
            retVal <- switch(returnType,
                RangoObject = retrieve(object, dbc, create = FALSE),
                data.frame = res,
                None = NULL)
            return(retVal)
          })
    })