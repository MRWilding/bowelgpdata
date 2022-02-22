library("dplyr")
library("tidyr")
library("janitor")
library("lubridate")
library("testthat")
library("purrr")

# Set column names for output from file
ColumnNames <- c("PracticeName", "PracticeCode", "60to69_eligible-on-last-day", 
                 "60to69_invited-to-screening-previous-12-months", "60to69_screened-within-6-months-of-invite", 
                 "60to69_screened-within-previous-30-months", "60to69_Uptake", "60to69_2.5-year-coverage", "60to74_eligible-on-last-day", 
                 "60to74_invited-to-screening-previous-12-months", "60to74_screened-within-6-months-of-invite", 
                 "60to74_screened-within-previous-30-months", "60to74_Uptake", "60to74_2.5-year-coverage") 

# Identify root folder containing files
folder <- "W:\\DataAndInfo_NDT\\NationalDataTeam_Restricted\\OpenExeter\\Bowel\\GPPP_2021Oct"

# make a list of directories in the flder
dirs <- list.dirs(folder, recursive = F)

# Find the files to have data extracted
for (dir in dirs) { # for each directory

  filelist <- list.files(dir, full.names = T, pattern = "\\.csv$", ignore.case = T) # make a list of the files in the folder

  files <- c(files, filelist) # append the files to the files object
  
}

files <- files[!grepl("RAW_", files)] # removes any files from the process if they have been re-saved as RAW files 

# Defines the path to use for data extraction
#path <- "W:\\DataAndInfo_NDT\\NationalDataTeam_Restricted\\OpenExeter\\Bowel\\GPPP_2021Oct\\Year_2010\\css_csv_export_bcs_report_Apr10.csv"

# Load Commissioning Organisation list
PCTData <- read.csv("Primary_Care_Organisations_(April_2011)_Names_and_Codes_in_England_.csv")

for (path in files) {
  
  # Identifies the directory to save the data in
  dataFolder <- dirname(path)
  
  # Sets folder location for output
  destinationFolder <- paste0(dataFolder, "\\ExtractedData\\")
  
  dateCell <- read.csv(path, header = F, nrow = 1) # reads the first cell of the csv
  dateText <- trimws(substr(dateCell[1], nchar(dateCell[1])-6, nchar(dateCell[1])-1)) #extracts date information from 
  
  extractYear <- as.integer(paste0("20", substr(dateText, nchar(dateText)-1, nchar(dateText)))) #identify year
  extractMonth <- match(tolower(substr(dateText, 1, nchar(dateText)-2)), tolower(month.abb)) # identify month
  
  # Set start and end date based on extracted values
  startDate <- as_date(ymd(paste0(extractYear, "-", extractMonth, "-", 1))) 
  endDate <- ceiling_date(startDate, 'month') - days(1)
  
  # Clean up environment
  rm(dateCell, extractMonth, extractYear, dateText)
  
  # reads data into the environment for the path, skipping the first 3 rows of the CSV to get column headings - converting blank cells to NA
  dataRaw <- read.csv(path, skip = 3, na.strings = c("NA",""))
  
  # Make updates to data to prepare for long table
  dataPCT <- dataRaw %>%
    select(!map(c("X."),
                starts_with, 
                vars = colnames(.)) %>%
             unlist(),
           -any_of(c("X"))) %>% # Removes Blank columns from raw data
    mutate(RegionType = names(.[1])) %>% # Sets the RegionType column to be the name of the first column
    rename(RegionName = names(.[1])) %>% # Renames the first column
    left_join(PCTData, by= c("Organisation.Code" = "PCO11CDO")) %>%
    rename(CommissioningOrganisation = PCO11NM) %>% # rename the column to fit the code
    mutate(CommissioningOrganisationCode = case_when(
      !is.na(CommissioningOrganisation) ~ Organisation.Code # Add Code data where it has been match
    ),
    CommissioningOrganisationType = "PCT") %>% # Add organisation type column manually (to be automated in future)
    select(c(1, 16, 18, 20:21, 2:15)) %>% # reorders the data frame to move the new columns to the organisation columns and drop unnecessary columns
    fill(RegionName, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType, .direction = "down") %>% # fill the data for the region and commissioning organisation down 
    filter(!is.na(Organisation)) %>% # remove the rows where there is no Organisation (Removes the blank region rows)
    rename(Practice = Organisation, PracticeCode = Organisation.Code)
  
  # Extracts rows where the Commissioning organisation matches the GP practice column  
  checkAggregationRaw <- dataPCT %>%
    filter(CommissioningOrganisationCode == PracticeCode) %>%
    select(-Practice, -PracticeCode)
  
  #Remove the rows where the commissioning organisation codes match the GP practice name (can aggregate up from GP data)
  dataFiltered <- dataPCT %>% filter(CommissioningOrganisationCode != PracticeCode) 
  
  # Calculate commissioning organisation data from GP practice data
  checkAggregationCalc <- dataFiltered %>%
    group_by(RegionName, RegionType, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType) %>%
    summarise_at(vars(No..of.eligible.people.on.last.day.of.review.period:X2.5.year.coverage...1), sum, na.rm = T) %>% # adds all numerical values within the groups
    group_by(.drop = "all") %>%
    # Calculates the uptake and coverage and formats to the same 1 d.p. as the raw data
    mutate(Uptake.. = round_half_up(No..of.people.screened.within.6.months.of.invitation / No..of.people.invited.for.screening.in.previous.12.months * 100, 1), 
           X2.5.year.coverage.. = round_half_up(No..of.people.screened.in.previous.30.months / No..of.eligible.people.on.last.day.of.review.period * 100, 1), 
           Uptake...1 = round_half_up(No..of.people.screened.within.6.months.of.invitation.1 / No..of.people.invited.for.screening.in.previous.12.months.1 * 100, 1),
           X2.5.year.coverage...1 = round_half_up(No..of.people.screened.in.previous.30.months.1 / No..of.eligible.people.on.last.day.of.review.period.1 * 100, 1))
  
  # Check that the removed raw data rows of commissioning organisations match the calculated values for those organisations
  errors <- setdiff(checkAggregationCalc, checkAggregationRaw) # identify different rows in Calc vs Raw
  errors2 <- setdiff(checkAggregationRaw, checkAggregationCalc) # identify different rows in Raw vs Calc
  
  if(nrow(errors) > 1 | nrow(errors2) > 1) {
    errors <- bind_rows(errors, errors2) # Bind any differences together to allow comparison by user
  }
  
  expect_equal(nrow(errors), 0, info = "Differences found between raw and calculated commissioning organisation lists.") # Check that there are no rows in the errors object and cause an error if there is
  
  # Clean up environment
  rm(errors, errors2, checkAggregationCalc, checkAggregationRaw, dataRaw, dataPCT)
  
  ColNamesAll <- c("RegionName", "RegionType", "CommissioningOrganisation", "CommissioningOrganisationCode", "CommissioningOrganisationType", ColumnNames) #create a list of column name
  
  names(dataFiltered) <- ColNamesAll # rename the columns based on the list
  
  # This section flips the data to long format, splitting the ages into a separate column from the column names, and then flips the values back out
  # to the numerical columns. Effectively it reduces the columns by adding the age data to a separate column
  dataLong <- dataFiltered %>%
    pivot_longer(cols = c(8:19), names_to = c("Age", "Category"), names_sep = "_") %>%
    pivot_wider(names_from = Category)
  
  # Clean up environment
  rm(dataFiltered, ColNamesAll)
  
  # Check that raw data uptake and coverage match calculated values
  
  dataLongCheck <- dataLong %>%
    mutate(UptakeCalc = round_half_up(replace_na(`screened-within-6-months-of-invite`/`invited-to-screening-previous-12-months`*100, 0), 1),
           CoverageCalc = round_half_up(replace_na(`screened-within-previous-30-months`/`eligible-on-last-day`*100, 0), 1))
  
  expect_equal(dataLongCheck$Uptake, dataLongCheck$UptakeCalc, info = "Calculated uptake values do not match the imported values")
  expect_equal(dataLongCheck$`2.5-year-coverage`, dataLongCheck$CoverageCalc, info = "Calculated coverage values do not match the imported values")
  
  rm(dataLongCheck) # clears the check object (note this only happens if there's no error)
  
  ws <- substring(dataLong$PracticeName[1], 1, 1)
  
  dataLong <- dataLong %>%
    # select(-Uptake, -`2.5-year-coverage`) %>%
    mutate(PracticeName = trimws(PracticeName, whitespace = ws),
           startDate = startDate,
           endDate = endDate) %>%
    select(c(1:7, 15:16), everything())
  
  # Saves the data as a CSV
  write.csv(dataLong, paste0(destinationFolder, "bowel-coverage-and-uptake-extracted-data_", formatC(month(startDate), width = 2, flag = 0), "-", year(startDate), ".CSV"), row.names = F)
  
  # Clean up environment
  rm(startDate, endDate, ws)
  
}

