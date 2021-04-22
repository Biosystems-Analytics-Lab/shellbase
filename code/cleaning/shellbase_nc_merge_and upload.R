### ----
# title: nc data cleaning - merge and upload to postgres routine and conditional data
# purpose:
# source: Andy Haines from NC Department of Marine Fisheries

# author: Megan Carr
# date created: 2021/03/09
# date last edited: 2021/03/09


### workspace set-up ----

# clear workspace
rm(list = ls())

# packages
library(tidyverse)
library(lubridate)
# database libraries
# library(odbc)
library(RPostgres)
library(DBI)
library(dbplyr)

### bind data ----

nc_fc <- bind_rows(routine_clean, conditional_clean) %>% arrange(date)
str(nc_fc)

# write csv
write_csv(nc_fc, path = "data/nc/nc_fc.csv")

### match database ----
nc_fc <- nc_fc %>%
  rename(area_name = grow_area,
         fc = fib_conc,
         sal = salinity,
         sample_datetime = date)

### connect to postgres ----

sort(unique(odbcListDrivers()[[1]]))

