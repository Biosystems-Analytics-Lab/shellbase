### ----
# title: nc data cleaning - routine monitoring
# purpose:
# source: Andy Haines from NC Department of Marine Fisheries

# author: Megan Carr
# date created: 2021/02/08
# date last edited: 2021/04/05


### workspace set-up ----

# clear workspace
rm(list = ls())

# packages
library(tidyverse)
library(lubridate)
library(janitor)
#install.packages("readxl")
library(readxl)
#install.packages("purrr")
library(purrr)



### load data ----

# initialize
folder_path <- "data/nc/routine_monitoring_ncdmf/"

file_names <- list.files(path=folder_path,pattern=".xlsx")
file_path <- paste0(folder_path,file_names)

# read-in loop
for (i in 1:length(file_names)){  
  dat_raw <- read_excel(file_path[i], sheet=1) %>%
    add_column(file_name = file_names[i]) %>%
    clean_names
  dat_name <- gsub(".xlsx","",file_names[i]) 
  assign(dat_name, dat_raw)
  }

# bind sampling dfs
routine <- bind_rows(SAMPLES1, SAMPLES2, SAMPLES3, SAMPLES4, SAMPLES5)

### explore fc data ----
str(routine)
summary(routine)

# see rows with NAs for `station` column
routine[is.na(routine$station),]

# see rows with NAs for `samp_date` column
routine[is.na(routine$samp_date),]

# see rows with NAs for `fc` column
routine[is.na(routine$fc),]

# find duplicate values
routine %>% 
  group_by(grow_area, station_id, id) %>% 
  filter(n()>1)
# no duplicates - ok to remove `id` column because it is a excel legacy column


### clean fc data ----

# remove `id` column, arrange in ascending order of `samp_date`, and add column for monitoring type
routine_clean <- routine %>%
  select(-(id)) %>%
  select(-(sta_code:station_id)) %>%
  select(-(comments)) %>%
  arrange(samp_date) %>%
  add_column(monitoring_type = rep("routine", times=nrow(routine)), .before = "grow_area") %>%
  rename(date = samp_date,
         fib_conc = fc)

# remove NAs in `fib_conc`
routine_clean <- routine_clean[!is.na(routine_clean$fib_conc), ]

# match columns to conditional
routine_clean <- routine_clean %>% add_column(alternative_station_name = rep(NA), .after = "grow_area")
routine_clean$alternative_station_name <- as.character(routine_clean$alternative_station_name)
routine_clean$station <- as.character(routine_clean$station)

# see final routine df
str(routine_clean)

### clean spatial data ----
stations <- STATIONS
