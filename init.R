options("h2o.use.data.table" = TRUE)
options(stringsAsFactors = FALSE)
options(java.parameters = "-Xmx4096m")
options(dplyr.width = Inf) 
Sys.setenv(TZ='GMT')
if( "data.table" %in% rownames(installed.packages()) ){
  data.table::setDTthreads(0)
}
if( "tidyverse" %in% rownames(installed.packages()) ){
  options(tidyverse.quiet = TRUE)
}

library(RMySQL)
library(tidyverse)

if(!"con" %in% ls()){
  con = dbConnect(
    dbDriver("MySQL"),
    dbname = "lab",
    user = "dfischer",
    password = "Danielfm123",
    host = "danielfm123.dlinkddns.com"
  )
}

