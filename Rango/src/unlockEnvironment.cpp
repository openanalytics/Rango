#include <Rcpp.h>
using namespace Rcpp;

/* This is taken from envir.c in the R 2.15.1 source
https://github.com/SurajGupta/r-source/blob/master/src/main/envir.c
*/
#define FRAME_LOCK_MASK (1<<14)
#define FRAME_IS_LOCKED(e) (ENVFLAGS(e) & FRAME_LOCK_MASK)
#define UNLOCK_FRAME(e) SET_ENVFLAGS(e, ENVFLAGS(e) & (~ FRAME_LOCK_MASK))

// [[Rcpp::export]]
RObject unlockEnvironment(Environment env){
  if (TYPEOF(env) == NILSXP)
	::Rf_error("use of NULL environment is defunct");
  if (TYPEOF(env) != ENVSXP)
    ::Rf_error("not an environment");

  UNLOCK_FRAME(env);
  // Return TRUE if unlocked; FALSE otherwise
  SEXP result = PROTECT( Rf_allocVector(LGLSXP, 1) );
  LOGICAL(result)[0] = FRAME_IS_LOCKED(env) == 0;
  UNPROTECT(1);
  return result;
}
