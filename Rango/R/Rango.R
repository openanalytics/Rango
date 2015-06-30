# Project: Rango
# 
# Author: Willem Ligtenberg - willem.ligtenberg@openanalytics.eu
###############################################################################

#' Generate a string representation for the object
#' @name stringRep
#' 
#' @param object object to represent as a string
#' @param dbc RangoConnection
#' @return string representation of the object
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @include postgres.R
#' @include sqlite.R
#' @export
setGeneric(
    name = "stringRep",
    def = function(object, dbc){standardGeneric("stringRep")})

#' Generate a string representation for the object
#' @return string representation of the object
#' @author Willem Ligtenberg
#' @importFrom Hmisc capitalize
setMethod(
    f = "stringRep",
    signature = "RangoObject",
    definition = function(object, dbc){
      pk <- getPrimaryKey(dbc, tolower(class(object)[1]))
      if(class(eval(substitute(object@x, list(x = pk)))) %in% 
          capitalize(listTables(dbc))){
        stringRep(eval(substitute(object@x, list(x = pk))), dbc)
        #TODO return something more useful than numeric
      } else{
        return(as.character(eval(substitute(object@x, list(x = pk)))))
      }
    })

#' Generate a string representation for the object
#' @return string representation of the object
#' @author Willem Ligtenberg
setMethod(
    f = "stringRep",
    signature = "numeric",
    definition = function(object, dbc){
        return(as.character(object))
    })

#' Generate a string representation for the object
#' @return string representation of the object
#' @author Willem Ligtenberg
setMethod(
    f = "stringRep",
    signature = "character",
    definition = function(object, dbc){
      return(object)
    })

#' Generate a string representation for the object
#' @return string representation of the object
#' @author Willem Ligtenberg
setMethod(
    f = "stringRep",
    signature = "logical",
    definition = function(object, dbc){
      return(as.character(object))
    })

#' Generate a string representation for the object
#' @return string representation of the object
#' @author Willem Ligtenberg
setMethod(
    f = "stringRep",
    signature = "POSIXct",
    definition = function(object, dbc){
      return(as.character(object))
    })

#' query the data base for objects with the specific attributes in 
#' the object that is passed.
#' @name retrieve
#' 
#' @param object object to retrieve
#' @param dbc RangoConnection
#' @param create Should a new entry be made in the data base if none exist
#' @param returnType How should the result be returned? Default is as 
#' RangoObject, None returns nothing (only useful for store), data.frame 
#' returns a data.frame 
#' @return returns (a list of) object with all the 
#' information from the data base (or a data.frame if chosen)
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @export
setGeneric(
    name = "retrieve",
    def = function(object, dbc, create = TRUE, returnType = "RangoObject"){
      standardGeneric("retrieve")})

#' query the data base for objects with the specific attributes in 
#' the object that is passed.
#' @return returns (a list of) object with all the 
#' information from the data base (or a data.frame if chosen)
#' @author Willem Ligtenberg
#' @importFrom RPostgreSQL dbGetQuery
#' @importFrom Hmisc capitalize
#' @importFrom logging logdebug
setMethod(
    f = "retrieve",
    signature = "RangoObject",
    definition = function(object, dbc, create = FALSE, 
        returnType = "RangoObject"){
      
      tableStatement <- getTableStatement(object, dbc, sql = TRUE)
      joinClauses <- getJoinClauses(object, dbc, sql = TRUE)
      constraints <- getConstraints(object, dbc, sql = TRUE)
      
      query <- paste0("SELECT ", tolower(class(object)), ".* FROM ",
          tableStatement, " WHERE ", if(joinClauses != ""){paste0(joinClauses, " AND ")}, constraints)
      
      logdebug(query)
      key <- paste(returnType, query, sep = "_")
      # Insert caching magic here
      if(is.null(dbc@objectCache[[key]])){
        res <- dbGetQuery(dbc@con, statement = query)
        retVal <- switch(returnType,
            RangoObject = createObjectFromResult(object, dbc, res, create),
            data.frame = res,
            None = NULL)
        # Save the object in the environment
        if(dbc@cache){
          assign(key, retVal, dbc@objectCache)
        }
        return(retVal)
      } else{
        return(dbc@objectCache[[query]])
      }
    })
    
#' query the data base for objects with the specific attributes in 
#' the object that is passed.
#' @return returns (a list of) object with all the 
#' information from the data base (or a data.frame if chosen)
#' @author Willem Ligtenberg
#' @importFrom Hmisc capitalize
#' @importFrom RPostgreSQL dbGetQuery
#' @importFrom logging logdebug
    setMethod(
        f = "retrieve",
        signature = "list",
        definition = function(object, dbc, create = FALSE, 
            returnType = "RangoObject"){
          logdebug("Retrieving a list")
          if(class(object[[1]])[1] %in% capitalize(listTables(dbc))){
            if(create){
              stop("Creation of multiple objects is not yet supported")
            } else{
              tableStatement <- getTableStatement(object, dbc, sql = TRUE)
              joinClauses <- getJoinClauses(object, dbc, sql = TRUE)
              constraints <- getConstraints(object, dbc, sql = TRUE)
              
              query <- paste0("SELECT ", tolower(class(object[[1]])), ".* FROM ",
                  tableStatement, " WHERE ", if(joinClauses != ""){paste0(joinClauses, " AND ")}, constraints)
            }
          }
          logdebug(query)
          key <- paste(returnType, query, sep = "_")
          # Insert caching magic here
          if(is.null(dbc@objectCache[[key]])){
            res <- dbGetQuery(dbc@con, statement = query)
            retVal <- switch(returnType,
                RangoObject = createObjectFromResult(object, dbc, res, create),
                data.frame = res,
                None = NULL)
            # Save the object in the environment
            if(dbc@cache){
              assign(key, retVal, dbc@objectCache)
            }
            return(retVal)
          } else{
            return(dbc@objectCache[[query]])
          }
        })

#' store the object to the data base
#' @name store
#' 
#' @param object object to store
#' @param dbc RangoConnection
#' @param force Should we create a new row even when 
#' something like this already exists
#' @param returnType How should the result be returned? Default is as 
#' RangoObject, None returns nothing (only useful for store), data.frame 
#' returns a data.frame 
#' @return returns (a list of) object with all the 
#' information from the data base (or a data.frame if chosen)
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @export
setGeneric(
    name = "store",
    def = function(object, dbc, force = FALSE, returnType = "RangoObject"){
      standardGeneric("store")})

#' detect non empty slots of an object
#' @name nonEmptySlots
#' 
#' @param object object to inspect
#' @param dbc RangoConnection to use
#' @return vector of non empty slots
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @export
#' @importFrom Hmisc capitalize
#' @importFrom methods S3Class slotNames
setGeneric(
    name = "nonEmptySlots",
    def = function(object, dbc){standardGeneric("nonEmptySlots")})

#' detect non empty slots of an object
#' @return vector of non empty slots
#' @author Willem Ligtenberg
#' @importFrom Hmisc capitalize
#' @importFrom methods S3Class slotNames
setMethod(
    f = "nonEmptySlots",
    signature = "RangoObject",
    definition = function(object, dbc){
      slotNames(object)[which(
              sapply(slotNames(object), function(x){
                    # Test for special slots
                    if(x %in% c("rangoBookKeeping")){
                      return(FALSE)
                    }
                    if(class(eval(parse(text = paste0("object@", x))))[1] %in% 
                        capitalize(listTables(dbc))){
                      if(identical(nonEmptySlots(eval(substitute(object@x, 
                                      list(x = x))), dbc), character(0))){
                        return(FALSE)
                      } else{
                        return(TRUE)
                      }
                    }else{
                      if("POSIXct" %in% attr(eval(parse(
                                  text = paste0("object@", x))), ".S3Class")){
                        return(FALSE)
                      }else{
                        obj <- eval(parse(text = paste0("object@", x)))
                        if(length(obj) != 0){
                          if(class(obj)[1] == "character"){
                            if(nchar(obj) == 0){
                              return(FALSE)
                            }
                          }
                          return(TRUE)
                        } else{
                          return(FALSE)
                        }
                      }
                    }}))]
    })

#' Return part of a SQL statetement that specifies the tables
#' @name getTableStatement
#' 
#' @param object object to inspect
#' @param dbc RangoConnection to use
#' @param sql Boolean, return a sql statement (TRUE), or list (FALSE)
#' @param tables List of tables already identified
#' @return String to insert in a SQL statement
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @export
setGeneric(
    name = "getTableStatement",
    def = function(object, dbc, sql = TRUE, tables = list()){
      standardGeneric("getTableStatement")})

#' Return part of a SQL statetement that specifies the tables
#' @return String to insert in a SQL statement
#' @author Willem Ligtenberg
setMethod(
    f = "getTableStatement",
    signature = "RangoObject",
    definition = function(object, dbc, sql = TRUE, tables = list()){
      values <- nonEmptySlots(object, dbc)
      indexForJoin <- sapply(values, function(x) 
            eval(substitute(inherits(object@x, "RangoObject"), list(x = x))))
      
      # Recurse into the objects
      for(i in indexForJoin){
        tables <- unlist(sapply(values[indexForJoin], 
                function(x) eval(substitute(getTableStatement(object@x, dbc, sql = FALSE, tables = tables), 
                          list(x = x)))))
      }
      tables <- c(tables, tolower(class(object)))
      if(sql){
        return(paste(unique(unlist(tables)), collapse = ", "))
      }else{
        return(tables)
      }
    })

#' Return part of a SQL statetement that specifies the tables
#' @return String to insert in a SQL statement
#' @author Willem Ligtenberg
setMethod(
    f = "getTableStatement",
    signature = "list",
    definition = function(object, dbc, sql = TRUE, tables = list()){
      allTables <- tables
      for(o in object){
        allTables <- c(allTables, getTableStatement(o, dbc, sql = FALSE, tables = allTables))
      }
      return(paste(unique(unlist(allTables)), collapse = ", "))
    })

#' Return part of a SQL statetement that specifies the join clauses
#' @name getJoinClauses
#' 
#' @param object object to inspect
#' @param dbc RangoConnection to use
#' @param sql Boolean, return a sql statement (TRUE), or list (FALSE)
#' @param clauses List of clauses already identified
#' @return String to insert in a SQL statement
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @export
setGeneric(
    name = "getJoinClauses",
    def = function(object, dbc, sql = TRUE, clauses = list()){
      standardGeneric("getJoinClauses")})

#' Return part of a SQL statetement that specifies the join clauses
#' @return String to insert in a SQL statement
#' @author Willem Ligtenberg
setMethod(
    f = "getJoinClauses",
    signature = "RangoObject",
    definition = function(object, dbc, sql = TRUE, clauses = list()){
      values <- nonEmptySlots(object, dbc)
      indexForJoin <- sapply(values, function(x) 
            eval(substitute(inherits(object@x, "RangoObject"), list(x = x))))
      
      clauses <- c(clauses, lapply(values[indexForJoin], function(x){
            paste0(tolower(class(object)), ".", x, " = ", 
                eval(substitute(tolower(class(object@x)), list(x = x))), 
                ".", getPrimaryKey(dbc, 
                    eval(substitute(tolower(class(object@x)), 
                            list(x = x)))))
          }))
      # Recurse into the objects
      for(i in which(indexForJoin)){
        clauses <- c(clauses, eval(substitute(getJoinClauses(object@x, dbc, sql = FALSE, clauses = clauses), 
                      list(x = values[i]))))
        clauses <- unique(clauses)
      }
      if(sql){
        return(paste(clauses, collapse = " AND "))
      }else{
        return(clauses)
      }
    })

#' Return part of a SQL statetement that specifies the join clauses
#' @return String to insert in a SQL statement
#' @author Willem Ligtenberg
setMethod(
    f = "getJoinClauses",
    signature = "list",
    definition = function(object, dbc, sql = TRUE, clauses = list()){
      for(o in object){
        clauses <- c(clauses, getJoinClauses(o, dbc, sql = FALSE, clauses = clauses))
      }
      return(paste(unique(unlist(clauses)), 
              collapse = " AND "))
    })

#' Return part of a SQL statetement that specifies the constraints
#' @name getConstraints
#' 
#' @param object object to inspect
#' @param dbc RangoConnection to use
#' @param sql Boolean, return a sql statement (TRUE), or list (FALSE)
#' @param clauses List of clauses already identified
#' @return String to insert in a SQL statement
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @export
setGeneric(
    name = "getConstraints",
    def = function(object, dbc, sql = TRUE, constraints = list()){
      standardGeneric("getConstraints")})

#' Return part of a SQL statetement that specifies the constraints
#' @return String to insert in a SQL statement
#' @author Willem Ligtenberg
setMethod(
    f = "getConstraints",
    signature = "RangoObject",
    definition = function(object, dbc, sql = TRUE, constraints = list()){
      values <- nonEmptySlots(object, dbc)
      indexForJoin <- sapply(values, function(x) 
            eval(substitute(inherits(object@x, "RangoObject"), list(x = x))))
      pkIndices <- values %in% getPrimaryKey(dbc, tolower(class(object)))
      
      indices <- !indexForJoin
      if(sum(pkIndices) > 0){
        indices <- pkIndices & !indexForJoin
      }

      for(v in values[indices]){
        key <- paste(tolower(class(object)), v, sep = ".")
        if(key %in% names(constraints)){
          constraints[[key]] <- append(constraints[[key]], list(eval(substitute(sqlEsc(object@x), list(x = v)))))
        } else{
          constraints[key] <- list(eval(substitute(sqlEsc(object@x), list(x = v))))
        }
      }
      
      # Recurse into the objects
      for(i in which(indexForJoin)){
        constraints <- eval(substitute(getConstraints(object@x, dbc, sql = FALSE, constraints = constraints), 
                          list(x = values[i])))
      }
      
      if(sql){
        retVal <- paste(lapply(names(constraints), function(x){
            if(length(constraints[[x]]) > 1){
              paste0(x, " IN ('", paste(unique(unlist(constraints[[x]])), 
                      collapse = "', '"), "')")
            }else{
              if(inherits(constraints[[x]], "RangoArgument")){
                paste(x, unique(unlist(constraints[[x]])))
              } else{
                paste0(x, " IN ('", paste(unique(unlist(constraints[[x]])), 
                        collapse = "', '"), "')")
              }
            }
          }), collapse = " AND ")
        return(retVal)
      }else{
        return(constraints)
      }
    })

#' Return part of a SQL statetement that specifies the constraints
#' @return String to insert in a SQL statement
#' @author Willem Ligtenberg
setMethod(
    f = "getConstraints",
    signature = "list",
    definition = function(object, dbc, sql = TRUE, constraints = list()){
      for(o in object){
        constraints <- getConstraints(o, dbc, sql = FALSE, constraints = constraints)
      }
      retVal <- paste(lapply(names(constraints), function(x){
          if(length(constraints[[x]]) > 1){
            paste0(x, " IN ('", paste(unique(unlist(constraints[[x]])), 
                    collapse = "', '"), "')")
          }else{
            if(inherits(constraints[[x]], "RangoArgument")){
              paste0(x, unique(unlist(constraints[[x]])))
            } else{
              paste0(x, " IN ('", paste(unique(unlist(constraints[[x]])), 
                      collapse = "', '"), "')")
            }
          }
        }), collapse = " AND ")
      return(retVal)
    })


#' Greater than function to allow specification arguments where you want some 
#' parameter to be greater than the following value.
#' @param var the name of the parameter
#' @param x the value of the parameter
#' @return a string that performs the correct sql query
#' @author Willem Ligtenberg
#' @export
`%>%` <- function(var, x){
  if(is.numeric(x) | inherits(x, "POSIXt")){
    retVal <- paste0(" > ", stringRep(x))
    names(retVal) <- deparse(substitute(var))
    class(retVal) <- c(class(x), "RangoArgument", class(retVal))
    return(retVal)
  } else {
    stop("greater than is only useful for numerics or dates")
  }
}

#' Greater than equals function to allow specification arguments where you want some 
#' parameter to be greater than or equal to the following value.
#' @param var the name of the parameter
#' @param x the value of the parameter
#' @return a string that performs the correct sql query
#' @author Willem Ligtenberg
#' @export
`%>=%` <- function(var, x){
  if(is.numeric(x) | inherits(x, "POSIXt")){
    retVal <- paste0(" >= ", stringRep(x))
    names(retVal) <- deparse(substitute(var))
    class(retVal) <- c(class(x), "RangoArgument", class(retVal))
    return(retVal)
  } else {
    stop("greater than or equal is only useful for numerics or dates")
  }
}

#' Smaller than function to allow specification arguments where you want some 
#' parameter to be smaller than the following value.
#' @param var the name of the parameter
#' @param x the value of the parameter
#' @return a string that performs the correct sql query
#' @author Willem Ligtenberg
#' @export
`%<%` <- function(var, x){
  if(is.numeric(x) | inherits(x, "POSIXt")){
    retVal <- paste0(" < ", stringRep(x))
    names(retVal) <- deparse(substitute(var))
    class(retVal) <- c(class(x), "RangoArgument", class(retVal))
    return(retVal)
  } else {
    stop("smaller than is only useful for numerics or dates")
  }
}

#' Smaller than equals function to allow specification arguments where you want some 
#' parameter to be smaller than or equal to the following value.
#' @param var the name of the parameter
#' @param x the value of the parameter
#' @return a string that performs the correct sql query
#' @author Willem Ligtenberg
#' @export
`%<=%` <- function(var, x){
  if(is.numeric(x) | inherits(x, "POSIXt")){
    retVal <- paste0(" <= ", stringRep(x))
    names(retVal) <- deparse(substitute(var))
    class(retVal) <- c(class(x), "RangoArgument", class(retVal))
    return(retVal)
  } else {
    stop("smaller than or equal is only useful for numerics or dates")
  }
}

#' Between function to allow specification arguments where you want some 
#' parameter to be between the values specified in the vector.
#' @param var the name of the parameter
#' @param x vector specifying the begin and end value
#' @return a string that performs the correct sql query
#' @author Willem Ligtenberg
#' @export
`%<=>%` <- function(var, x){
  if(is.numeric(x) | inherits(x, "POSIXt")){
    retVal <- paste0(" BETWEEN ", stringRep(x[1]), " AND ", stringRep(x[2]), " ")
    names(retVal) <- deparse(substitute(var))
    class(retVal) <- c(class(x), "RangoArgument", class(retVal))
    return(retVal)
  } else {
    stop("smaller than or equal is only useful for numerics or dates")
  }
}

#' Like function to allow specification arguments for partial matching.
#' @param var the name of the parameter
#' @param x vector string to match
#' @return a string that performs the correct sql query
#' @author Willem Ligtenberg
#' @export
`%like%` <- function(var, x){
  if(is.character(x)){
    retVal <- paste0(" LIKE '", stringRep(x), "' ")
    names(retVal) <- deparse(substitute(var))
    class(retVal) <- c(class(x), "RangoArgument", class(retVal))
    return(retVal)
  } else {
    stop("Like is only useful for character strings")
  }
}

#' Create an object from the result of the query
#' @param object The object that was stored or retrieved 
#' @param dbc database connection to use
#' @param result the result of the query
#' @param create Should we create the object if it did not exist?
#' @return Object itself
#' @author Willem Ligtenberg
createObjectFromResult <- function(object, dbc, result, create) {
  if(!class(object)[1] %in% capitalize(listTables(dbc))){
    object <- object[[1]]
  } else{
    if(!inherits(object, "RangoObject")){
      logdebug("This function should only receive lists of RangoObjects or RangoObjects itself")
    }
  }
  if(nrow(result) > 0){
    # object exists
    resList <- list(nrow(result))
    for(i in seq_len(nrow(result))){
      nonNaRes <- result[i, !is.na(result[i, ])]
      arguments <- paste(sapply(seq_len(ncol(nonNaRes)), 
              function(x, nonNaRes, object, dbc) 
                argumentFormatter(colnames(nonNaRes)[x], 
                    nonNaRes[, x],
                    object, dbc), nonNaRes, object, dbc), 
          collapse = ", ")
      cmd <- paste0(tolower(class(object)), "(", arguments, ")")
      logdebug(cmd)
      resultItem <- eval(parse(text = cmd), envir = .GlobalEnv)
      resultItem@rangoBookKeeping[["retrieved"]] <- TRUE
      resultItem@rangoBookKeeping[["dbc"]] <- dbc
      resList[i] <- resultItem
    }
    if(nrow(result) == 1){
      resList <- resList[[1]]
    }
    return(resList)
  } else{
    # object did not exist in database
    if(create){
      return(store(object, dbc))
    } else{
      return(NA)
    }
  }
}