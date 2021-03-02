### ----
# title: nc data cleaning - routine monitoring
# purpose:
# source: Andy Haines from NC Department of Marine Fisheries

# author: Megan Carr
# date created: 2021/02/08
# date last edited: 2021/03/02


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


# user defined functions ----

# reads all sheets in an excel file into R as tibble(s)
read_excel_allsheets <- function(path, tibble = TRUE){
  sheets <- readxl::excel_sheets(path)
  sheet_count <- 1:length(sheets)
  
  if(length(sheets) == 1) {
    # dat_1 <- read_excel(path, sheet = 1)
    list_dat <- lapply(sheets, function(X) readxl::read_excel(path, sheet = X))
    sheet_name <- paste0("dat", "_", sheet_count)
    names(list_dat) <- sheet_name # because names(x) must be a character vector of the same length as x
    list2env(list_dat, envir = .GlobalEnv)
    
  } else if (length(sheets) == 2) {
    list_dat <- lapply(sheets, function(X) readxl::read_excel(path, sheet = X))
    sheet_name <- paste0("dat", "_", sheet_count)
    names(list_dat) <- sheet_name # because names(x) must be a character vector of the same length as x
    list2env(list_dat, envir = .GlobalEnv)
    
  } else if (length(sheets) == 3) {
    list_dat <- lapply(sheets, function(X) readxl::read_excel(path, sheet = X))
    sheet_name <- paste0("dat", "_", sheet_count)
    names(list_dat) <- sheet_name
    list2env(list_dat, envir = .GlobalEnv)
    
  } else if (length(sheets) == 4) {
    list_dat <- lapply(sheets, function(X) readxl::read_excel(path, sheet = X))
    sheet_name <- paste0("dat", "_", sheet_count)
    names(list_dat) <- sheet_name
    list2env(list_dat, envir = .GlobalEnv)
    
  } else {
    print("ERROR: Excel file contains more than 4 sheets. File path is")
    print(path)
  }
  
}


### load conditional data ----

# initialize
folder_path <- "data/nc/conditional_sampling_ncdmf/datasheets/"
file_names <- list.files(path=folder_path,pattern=".xls") #this also encompasses ".xlsx"
file_path <- paste0(folder_path,file_names)

# identify grow area from file name
grow_area <- file_names %>% substr(start = 6, stop = 7) %>% str_to_upper(.)

# conditional read-in loop ----
conditional <- tibble()

for (i in 1:length(file_names)){  
  
  # read in file and define df per sheet (dat_1, dat_2, dat_3...)
  read_excel_allsheets(path = file_path[i])
  print(file_path[i])  # if an error occurs, the last file printed will be the cause of the error
  
  # counts number of sheets for nested for loop
  sheets <- readxl::excel_sheets(file_path[i])
  
  # nested loop to clean df
  for (j in 1:length(sheets)){
    
    print(j) # if an error occurs, the last sheet printed will be the cause of the error
    # call for the df to clean
    dat <- get(paste0("dat_",j))
    
    # rename or add station name in 2018, depending on if the column exists
    # whether the column exists varies between sheets in one excel file
    if ("2018 Sta. #" %in% colnames(dat)) {
      dat <- dat %>% rename("station_no_2018" = "2018 Sta. #")
    } else if ("New Station" %in% colnames(dat)) {
      dat <- dat %>% rename("station_no_2018" = "New Station")
    } else if ("NEW STATION" %in% colnames(dat)) {
      dat <- dat %>% rename("station_no_2018" = "NEW STATION")
    } else {
      dat <- dat %>% add_column("station_no_2018" = rep(NA), .before = colnames(dat)[1])
    }
    
    # pivot data longer  
    dat <- dat %>% pivot_longer(cols = 4:ncol(dat),
                                names_to = "date",
                                names_transform = list(date = as.numeric),
                                values_to = "fib_conc",
                                values_transform = list(fib_conc = as.numeric))
    
    # convert dates in numeric to date format
    # must occur after pivoting because variable names cannot be converted (no data type - the observations of the variables are what is associated with data type)
    dat$date <- excel_numeric_to_date(as.numeric(dat$date), date_system = "modern")
    
    # ensure that `station` variable is character type
    dat$STATION <- as.character(dat$STATION)
    
    # add columns of conditional sampling, growing area, and original file name
    dat <- dat %>%
      add_column(grow_area = rep(grow_area[i], times=nrow(dat)), .before = "station_no_2018") %>%
      separate(col = grow_area, into = c("monitoring_type", "grow_area"), sep = "_") %>%
      add_column(file_name = rep(file_names[i], times=nrow(dat)), .after = "fib_conc") %>%
      rename("station" = "STATION", "number" = "NO.")
    
    # bind data
    conditional <- bind_rows(conditional, dat)
    
  } # end nested loop
  
  # reset loop by removing `dat`, etc
  rm(dat, dat_1)
  if(exists("dat_2")) rm(dat_2)
  if(exists("dat_3")) rm(dat_3)
  if(exists("dat_4")) rm(dat_4)
  
} # end read-in loop


