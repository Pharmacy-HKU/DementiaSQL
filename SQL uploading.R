## Script name:  Dementia cohort
## Purpose of script: uploading dementia cohort to SQL
## Author: FAN Min
## Date Created: 2023-02-20
## Copyright (c) FAN Min, 2023
## Email: minfan@connect.hku.hk
##
## Notes:
# Tue Feb 21 18:37:21 2023 ------------------------------
##   1. Check if there is a huge time difference in R and SQL server (Yang)
##   2. double checking the whole uploading process see if there are missing
##   3. fix the op (they are with different structure) and upload 
##  

source("rsqlenv.R")

# all rx uploading --------------------------------------------------------

all_rx <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/All Rx_01_20",recursive = T,full.names = T)
# 3028 -2 = 3026
j <- 0 # index number
k <- c() # nrows
s <- Sys.time() # testing for time consuming
# Error happen in 1784 1201 cos those two are started with "~", which means they
# are opened by somebody else. In the following codes, those names with "~" will
# be removed in advance.
for(i in all_rx){
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


# all dx ------------------------------------------------------------------

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


# all ip ------------------------------------------------------------------
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



# all fm ------------------------------------------------------------------
# family module = fm. not me :)

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


# cohort file -------------------------------------------------------------

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



## I dont have time to fix the OP files cos they are with mixed structure. And
## OP should be not as important as others.

# all_op <- list.files(pattern = "conv.xlsx","M:/Cohort Raw Data (do not edit)/Dementia cohort/CDARS/converted/01-20 All OP//",recursive = T,full.names = T)
# all_op <- all_op[!grepl("~",all_op)]
# 
# j <- 0
# k <- c()
# s <- Sys.time()
# for(i in all_cohort){
#     start.time <- Sys.time()
#     j <- j+1
#     temp <- as.data.table(readxl::read_excel(i))
#     colnames(temp) <- janitor::make_clean_names(colnames(temp))
#     temp[,reference_key:=as.numeric(reference_key)]
#     temp[,source:=strsplit(i,"/")[[1]][7]]
#     temp[,study:="dementia"]
#     temp[,PI:="Celine"]
#     k <- append(k,nrow(temp))
#     dbWriteTable(connec
#                  # schema and table
#                  , c("CDARS", "cohort")
#                  , temp
#                  , append = TRUE # add row to bottom
#                  , row.names = FALSE
#     )
#     end.time <- Sys.time()
#     time.taken <- end.time - start.time
#     message(paste(j,length(all_cohort),sep='/'), 
#             " | time cost:",round(as.numeric(time.taken),2),
#             "s | nrow:",nrow(temp)," | ",strsplit(i,"/")[[1]][7])
#     
# }
# Sys.time() - s



