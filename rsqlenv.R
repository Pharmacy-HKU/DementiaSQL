# SQL env


# Package loading and environment setting ----------------------------------
options(scipen = 6, digits = 4) 
memory.limit(30000000)   

library(data.table)
library(RPostgreSQL) # package for connecting with D24H
library(rstudioapi)  # for password typing


tryCatch({
    drv <- dbDriver("PostgreSQL")
    print("Connecting to Database")
    connec <- dbConnect(drv, 
                        dbname = "d24h_prog4_db1",   # Specify the name of your Database
                        host = "hpc.d24h.hk"  ,  # Specify host name e.g.:"aws-us-east-1-portal.4.dblayer.com"
                        port = "24543", # Specify your port number. e.g. 98939
                        user = "minfan", # Specify your username. e.g. "admin"
                        password = rstudioapi::askForPassword("password!"),
                        options="-c search_path=cdars")
    print("Database Connected!")
},
error=function(cond) {
    print("Unable to connect to Database.")
})

# time costing  ----------------------------------------------------------------
tc <- function(x){
    start_time <- Sys.time()
    x
    end_time <- Sys.time()
    end_time - start_time
}