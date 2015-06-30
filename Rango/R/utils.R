# Project: Rango
# 
# Author: Willem Ligtenberg - willem.ligtenberg@openanalytics.eu
###############################################################################

#' generate correct string representation of the object
#' @param object object to format
#' @param dbc RangoConnection
#' @return string representation of the object
#' @author Willem Ligtenberg
#' @export
#' @importFrom Hmisc capitalize
formatter <- function(object, dbc){
  objectClass <- class(object)[1]
  value <- NULL
  value <- switch(objectClass,
      'numeric' = object,
      'character' = paste0("'", sqlEsc(object), "'"),
      'logical' = object,
      'POSIXct' = paste0("'", sqlEsc(as.character(object)), "'"),
      'integer' = object)
  if(is.null(value) & objectClass %in% capitalize(listTables(dbc))){
    value <- stringRep(object, dbc)
  }
  return(value)
}

#' generate string that can be used as arguments
#' @param field which db field is used
#' @param item the item itself
#' @param object object it is associated with
#' @param dbc RangoConnection
#' @return string that can be used as arguments
#' @author Willem Ligtenberg
#' @export
#' @importFrom Hmisc capitalize
#' @importFrom methods new S3Class
argumentFormatter <- function(field, item, object, dbc){
  objectClass <- class(eval(parse(text = paste0("object@", field))))[1]
  if(objectClass == "S4"){
    objectClass <- attr(eval(
            parse(text = paste0("object@", field))), ".S3Class")[1]
  }
  value <- NULL
  value <- switch(objectClass,
      'integer' = item,
      'numeric' = item,
      'character' = paste0("'", rEsc(item), "'"),
      'logical' = item,
      'POSIXct' = paste0("as.POSIXct('", sqlEsc(as.character(item)), "')"))
  if(is.null(value) & objectClass %in% capitalize(listTables(dbc))){
    pk <- getPrimaryKey(dbc, tolower(objectClass))
    test <- eval(substitute(new(Class = x)@pk, 
            list(x = objectClass[1], pk = pk)))
    if(class(test)[1] %in% capitalize(listTables(dbc))){
      value <- paste0(tolower(objectClass), "(", pk, " = ", 
          paste0(tolower(class(test)[1]), "(", argumentFormatter(
                  getPrimaryKey(dbc, tolower(class(test)[1])), 
                  item, test, dbc), ")"), ")")
    } else{
      value <- paste0(tolower(objectClass), 
          "(", pk, " = ", item, ")")
    }
  }
  retVal <- paste0(field, " = ", 
      value
  )
  return(retVal)
}

#' escape some nasties in SQL strings
#' @param s String to escape
#' @return escaped string
#' @author Willem Ligtenberg
#' @export
#' @importFrom gdata trim
sqlEsc <- function(s){
  if(!inherits(s, "RangoArgument")){
    s <- gsub("\\\\", "\\\\\\\\", trim(s))
    s <- gsub("'", "''", s)
    s <- gsub('"', '""', s)
    s <- gsub("NULL", "\\\\NULL'", s)
  }
  return(s)
}

#' escape some quotes for R string
#' @param s String to escape
#' @return escaped string
#' @author Willem Ligtenberg
#' @export
#' @importFrom gdata trim
rEsc <- function(s){
  s <- gsub("'", "\\\\'", trim(s))
  s <- gsub('"', '\\\\"', s)
  return(s)
}

#' Get the primary key for a table
#' @name getPrimaryKey
#' 
#' @param dbc database connection to use
#' @param tableName name of the table
#' @return string that contains the primary key of the table
#' 
#' @author Willem Ligtenberg
#' @docType methods
#' @export
setGeneric(
    name = "getPrimaryKey",
    def = function(dbc, tableName){standardGeneric("getPrimaryKey")})

#' Function to force the assignment of new items in the Rango namespace
#' Adapted from Rcpp
#' @param x 
#' @param value 
#' @author Willem Ligtenberg
#' @useDynLib Rango
#' @importFrom Rcpp sourceCpp
forceAssignMyNamespace <- function(x, value){
  if(x %in% ls(.getNamespace("Rango"))){
    warning(paste0("Table name clashes with internal functions, ",
            "please use generateClasses to generate the R code and either ",
            "source that code, or include that in your package."))
  }else{
    unlocker <- get( "unlockBinding", baseenv() )
    if(exists(x, envir = .getNamespace("Rango"), inherits = FALSE) && 
        bindingIsLocked(x, .getNamespace("Rango"))){
      unlocker(x, .getNamespace("Rango"))
    }
    assign(x, value, .getNamespace("Rango"))
    lockBinding(x, .getNamespace("Rango"))
  }
}
