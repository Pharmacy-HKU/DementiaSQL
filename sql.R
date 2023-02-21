## Script name:  Dementia cohort
## Purpose of script: uploading dementia cohort to SQL
## Author: FAN Min
## Date Created: 2023-02-20
## Copyright (c) FAN Min, 2023
## Email: minfan@connect.hku.hk
##
## Notes:
##   
##




# Package loading and enviroment setting ----------------------------------
options(scipen = 6, digits = 4) 
memory.limit(30000000)

library(data.table) # package for cleanning
# source("functions/packages.R")       # loads up all the packages we need
library(RPostgreSQL)


tryCatch({
    drv <- dbDriver("PostgreSQL")
    print("Connecting to Database…")
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


all_rx <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/All Rx_01_20",recursive = T,full.names = T)

library(data.table)
j <- 0
k <- c()
s <- Sys.time()
# 1784 1201
for(i in all_rx[1785:3028]){
    start.time <- Sys.time()
    j <- j+1
    temp <- as.data.table(readxl::read_excel(i))
    colnames(temp) <- janitor::make_clean_names(colnames(temp))
    temp[,reference_key:=as.numeric(reference_key)]
    temp[,dispensing_duration:=as.numeric(dispensing_duration)]
    temp[,type_of_patient_drug:=gsub("\n","",type_of_patient_drug)]
    temp[,source:=strsplit(i,"/")[[1]][8]]
    temp[,study:="dementia"]
    temp[,PI:="Celine"]
    k <- append(k,nrow(temp))
    dbWriteTable(connec
                 # schema and table
                 , c("CDARS", "Rx")
                 , temp
                 , append = TRUE # add row to bottom
                 , row.names = FALSE
    )
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    message(paste(j,length(all_rx),sep='/'), 
            " | time cost:",round(as.numeric(time.taken),2),
            "s | nrow:",nrow(temp)," | ",strsplit(i,"/")[[1]][8])
    
}
Sys.time() - s


all_dx <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/All Dx_01_20/",recursive = T,full.names = T)
all_dx <- all_dx[!grepl("~",all_dx)]

j <- 0
k <- c()
s <- Sys.time()
for(i in all_dx){
    start.time <- Sys.time()
    j <- j+1
    temp <- as.data.table(readxl::read_excel(i))
    colnames(temp) <- janitor::make_clean_names(colnames(temp))
    temp[,reference_key:=as.numeric(reference_key)]
    temp[,patient_type_ip_op_a_e:=gsub("\n","",patient_type_ip_op_a_e)]
    temp[,source:=strsplit(i,"/")[[1]][7]]
    temp[,study:="dementia"]
    temp[,PI:="Celine"]
    k <- append(k,nrow(temp))
    dbWriteTable(connec
                 # schema and table
                 , c("CDARS", "Dx")
                 , temp
                 , append = TRUE # add row to bottom
                 , row.names = FALSE
    )
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    message(paste(j,length(all_dx),sep='/'), 
            " | time cost:",round(as.numeric(time.taken),2),
            "s | nrow:",nrow(temp)," | ",strsplit(i,"/")[[1]][7])
    
}
Sys.time() - s




all_ip <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/All IP_01_20/",recursive = T,full.names = T)
all_ip <- all_ip[!grepl("~",all_ip)]

j <- 0
k <- c()
s <- Sys.time()
for(i in all_ip){
    start.time <- Sys.time()
    j <- j+1
    temp <- as.data.table(readxl::read_excel(i))
    colnames(temp) <- janitor::make_clean_names(colnames(temp))
    temp[,reference_key:=as.numeric(reference_key)]
    temp[,comment:=gsub("\n","",comment)]
    temp[,source:=strsplit(i,"/")[[1]][7]]
    temp[,study:="dementia"]
    temp[,PI:="Celine"]
    k <- append(k,nrow(temp))
    dbWriteTable(connec
                 # schema and table
                 , c("CDARS", "Ip")
                 , temp
                 , append = TRUE # add row to bottom
                 , row.names = FALSE
    )
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    message(paste(j,length(all_ip),sep='/'), 
            " | time cost:",round(as.numeric(time.taken),2),
            "s | nrow:",nrow(temp)," | ",strsplit(i,"/")[[1]][7])
    
}
Sys.time() - s



all_fm <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/All FM_01_20/",recursive = T,full.names = T)
all_fm <- all_fm[!grepl("~",all_fm)]

j <- 0
k <- c()
s <- Sys.time()
for(i in all_fm){
    start.time <- Sys.time()
    j <- j+1
    temp <- as.data.table(readxl::read_excel(i))
    colnames(temp) <- janitor::make_clean_names(colnames(temp))
    temp[,reference_key:=as.numeric(reference_key)]
    temp[,fm_module_health_status_examination_investigation_result_date:=gsub("\r\n","",fm_module_health_status_examination_investigation_result_date)]
    temp[,source:=strsplit(i,"/")[[1]][7]]
    temp[,study:="dementia"]
    temp[,PI:="Celine"]
    k <- append(k,nrow(temp))
    dbWriteTable(connec
                 # schema and table
                 , c("CDARS", "Fm")
                 , temp
                 , append = TRUE # add row to bottom
                 , row.names = FALSE
    )
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    message(paste(j,length(all_fm),sep='/'), 
            " | time cost:",round(as.numeric(time.taken),2),
            "s | nrow:",nrow(temp)," | ",strsplit(i,"/")[[1]][7])
    
}
Sys.time() - s






all_cohort <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/dementia cohort_01_20/",recursive = T,full.names = T)
all_cohort <- all_cohort[!grepl("~",all_cohort)]

j <- 0
k <- c()
s <- Sys.time()
for(i in all_cohort){
    start.time <- Sys.time()
    j <- j+1
    temp <- as.data.table(readxl::read_excel(i))
    colnames(temp) <- janitor::make_clean_names(colnames(temp))
    temp[,reference_key:=as.numeric(reference_key)]
    temp[,source:=strsplit(i,"/")[[1]][7]]
    temp[,study:="dementia"]
    temp[,PI:="Celine"]
    k <- append(k,nrow(temp))
    dbWriteTable(connec
                 # schema and table
                 , c("CDARS", "cohort")
                 , temp
                 , append = TRUE # add row to bottom
                 , row.names = FALSE
    )
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    message(paste(j,length(all_cohort),sep='/'), 
            " | time cost:",round(as.numeric(time.taken),2),
            "s | nrow:",nrow(temp)," | ",strsplit(i,"/")[[1]][7])
    
}
Sys.time() - s




all_op <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/01-20 All OP//",recursive = T,full.names = T)
all_op <- all_op[!grepl("~",all_op)]

j <- 0
k <- c()
s <- Sys.time()
for(i in all_cohort){
    start.time <- Sys.time()
    j <- j+1
    temp <- as.data.table(readxl::read_excel(i))
    colnames(temp) <- janitor::make_clean_names(colnames(temp))
    temp[,reference_key:=as.numeric(reference_key)]
    temp[,source:=strsplit(i,"/")[[1]][7]]
    temp[,study:="dementia"]
    temp[,PI:="Celine"]
    k <- append(k,nrow(temp))
    dbWriteTable(connec
                 # schema and table
                 , c("CDARS", "cohort")
                 , temp
                 , append = TRUE # add row to bottom
                 , row.names = FALSE
    )
    end.time <- Sys.time()
    time.taken <- end.time - start.time
    message(paste(j,length(all_cohort),sep='/'), 
            " | time cost:",round(as.numeric(time.taken),2),
            "s | nrow:",nrow(temp)," | ",strsplit(i,"/")[[1]][7])
    
}
Sys.time() - s




# extract -----------------------------------------------------------------

test <- dbGetQuery(connec, "SELECT * FROM minfan_schema.cohort;")
test <- dbGetQuery(connec, "SELECT * FROM CDARS.cohort;")

dbGetQuery(connec, "SELECT count(reference_key) FROM \"CDARS\".cohort;")
dbGetQuery(connec, "SELECT count(subject_id) FROM minfan_schema.cohort;")

dbGetQuery(connec, "SELECT DISTINCT reference_key FROM \"CDARS\".cohort;")


# dplyr -------------------------------------------------------------------

library(dplyr)
library(rstudioapi)
# Connect to local PostgreSQL via dplyr
localdb <- src_postgres(dbname = 'd24h_prog4_db1',
                        host = 'hpc.d24h.hk',
                        port = 24543,
                        user = 'minfan',
                        password = askForPassword(prompt = "Please enter your password"))
d <- tbl(localdb, dbplyr::in_schema('cdars','cohort'))

dtab <-  as.data.frame(d)


# send a query through dplyr
query = "select DISTINCT (reference_key)          from \"cdars\".cohort"
dsub = tbl(localdb, sql(query))
dsub

#CREATE INDEX friends_city_desc ON friends(city DESC);

tbl(localdb,sql("CREATE TABLE cohort_index LIKE cohort;"))
dbcre(localdb, "CREATE TABLE cohort_index LIKE cohort;")


# dbSendQuery(connec, "CREATE TABLE cdars.cohort_index AS (SELECT * FROM  cdars.cohort);")
dbGetQuery(connec,"Select count(reference_key) from cdars.cohort_index;")


dbSendQuery(connec, "CREATE INDEX refkey ON cdars.rx(reference_key);")
dbSendQuery(connec, "DROP INDEX refkey;")
dbGetQuery(connec,"Select count(reference_key) from cdars.Rx;")




# testing -----------------------------------------------------------------
time_spent <- function(x){
    start_time <- Sys.time()
    x
    end_time <- Sys.time()
    end_time - start_time
}
time_spent(dbGetQuery(connec,"Select * from cdars.rx where reference_key=12667677;"))
time_spent(dbGetQuery(connec,"Select * from cdars.rx where reference_key=1469139;"))

time_spent(dbGetQuery(connec,"Select * from cdars.rx where reference_key=12667677 and action_status ='Issued' 
                      and prescription_start_date > '2010-5-1';"))



dbGetQuery(connec,
           "SELECT table_name FROM cdars.tables
                   WHERE table_schema='cdars'")

dbListFields(connec, c("cdars", "cohort"))

df <- dbGetQuery(connec, "SELECT * FROM cdars.cohort;")
