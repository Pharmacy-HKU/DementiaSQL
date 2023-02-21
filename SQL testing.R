## Script name:  SQL testing
## Purpose of script: testng codes for Dementia SQL database; Check if SQL 
##                    can save time than local PC
## Author: FAN Min
## Date Created: 2023-02-21
## Copyright (c) FAN Min, 2023
## Email: minfan@connect.hku.hk
##
## Notes:
# Tue Feb 21 18:39:03 2023 ------------------------------
##   1. the database with Index is obviously faster than without Index
##   2. Check if only RpostgreSQL is enough? or we still need src_postgres?
##      I perfer using one package to solve the problem
##   3. Check if we need to use dbplyr for beginner?


source("rsqlenv.R")


# time costing  -----------------------------------------------------------

tc(dbGetQuery(connec,"Select * from cdars.rx where reference_key=12667677;"))
tc(dbGetQuery(connec,"Select * from cdars.rx where reference_key=1469139;"))
tc(dbGetQuery(connec,"Select * from cdars.rx where reference_key=12667677 and action_status ='Issued' 
                      and prescription_start_date > '2010-5-1';"))


dbListFields(connec, c("cdars", "cohort")) # get the column names for cohort



# extract -----------------------------------------------------------------
dbGetQuery(connec, "SELECT DISTINCT reference_key FROM cdars.cohort;")
dbGetQuery(connec, "SELECT count(reference_key) FROM cdars.cohort;")
dbGetQuery(connec, "SELECT count(subject_id) FROM minfan_schema.cohort;")

mfschema <- dbGetQuery(connec, "SELECT * FROM minfan_schema.cohort;")
df <- dbGetQuery(connec, "SELECT * FROM cdars.cohort;") # obtain the cohort



# dplyr -------------------------------------------------------------------
library(dplyr)
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



