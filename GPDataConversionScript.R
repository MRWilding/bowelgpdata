# Load packages using groundhog package
groundhogDay <- "2021-10-01"

library(groundhog)

groundhog.library(c("dplyr", "tidyr", "testthat"), groundhogDay)

# Set column names for output from file
ColumnNames <- c("PracticeName", "PracticeCode", "60to69_eligible-on-last-day", 
                 "60to69_invited-to-screening-previous-12-months", "60to69_screened-within-6-months-of-invite", 
                 "60to69_screened-within-previous-30-months", "60to69_Uptake", "60to69_2.5-year-coverage", "60to74_eligible-on-last-day", 
                 "60to74_invited-to-screening-previous-12-months", "60to74_screened-within-6-months-of-invite", 
                 "60to74_screened-within-previous-30-months", "60to74_Uptake", "60to74_2.5-year-coverage") 

#defines the path to use for data extraction
path <- "W:\\DataAndInfo_NDT\\NationalDataTeam_Restricted\\OpenExeter\\Bowel\\GPPP_2021Oct\\Year_2010\\css_csv_export_bcs_report_Apr10.csv"

#reads data into the environment for the path, skipping the first 3 rows of the CSV to get column headings - converting blank cells to NA
dataRaw <- read.csv(path, skip = 3, na.strings = c("NA",""))

PCTData <- read.csv("Primary_Care_Organisations_(April_2011)_Names_and_Codes_in_England_.csv")

data <- dataRaw %>%
  mutate(RegionType = names(test[1])) %>% # Sets the RegionType column to be the name of the first column
  rename(RegionName = names(test[1])) %>% # Renames the first column
  mutate(CommissioningOrganisation = case_when( # Adds a column of names based on the PCT, CCG or STP the data contains
    endsWith(test$Organisation, "PCT") ~ Organisation,
    endsWith(test$Organisation, "CCG") ~ Organisation,
    endsWith(test$Organisation, "STP") ~ Organisation
  )) %>%
  mutate(CommissioningOrganisationType = case_when( # Adds a column type of organisation based on the end of the name
    endsWith(test$Organisation, "PCT") ~ "PCT",
    endsWith(test$Organisation, "CCG") ~ "CCG",
    endsWith(test$Organisation, "STP") ~ "STP"
  )) %>%
  mutate(CommissioningOrganisationCode = ifelse(CommissioningOrganisation == Organisation, Organisation.Code, NA)) %>% # Adds the codes for the organisations
  select(RegionName, RegionType, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType, everything()) %>% # reorders the data frame to move the new columns to the organisation columns
  fill(RegionName, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType, .direction = "down") %>% # fill the data for the region and commissioning organisation down 
  filter(!is.na(Organisation)) # remove the rows where there is no Organisation (Removes the blank region rows)

# Using PCT code from file

dataPCT <- dataRaw %>%
  mutate(RegionType = names(test[1])) %>% # Sets the RegionType column to be the name of the first column
  rename(RegionName = names(test[1])) %>% # Renames the first column
  left_join(PCTData, by= c("Organisation.Code" = "PCO11CDO")) %>%
  rename(CommissioningOrganisation = PCO11NM) %>% # rename the column to fit the code
  mutate(CommissioningOrganisationCode = case_when(
    !is.na(CommissioningOrganisation) ~ Organisation.Code # Add Code data where it has been match
    ),
    CommissioningOrganisationType = "PCT") %>% # Add organisation type column manually (to be automated in future)
  select(c(1, 17, 19, 21:22, 2:16)) %>% # reorders the data frame to move the new columns to the organisation columns and drop unnecessary columns
  fill(RegionName, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType, .direction = "down") %>% # fill the data for the region and commissioning organisation down 
  filter(!is.na(Organisation)) %>% # remove the rows where there is no Organisation (Removes the blank region rows)
  rename(GPPractice = Organisation)

# Extracts rows where the Commissioning organisation matches the GP practice column  
checkAggregationRaw <- dataPCT %>%
  filter(CommissioningOrganisationCode == Organisation.Code) %>%
  select(-Organisation, -Organisation.Code, - X)

dataFiltered <- dataPCT %>% filter(CommissioningOrganisationCode != Organisation.Code) %>% #Remove the rows where the commissioning organisation codes match the GP practice name (can aggregate up from GP data)
  select(-ncol(test2)) #drop the empty row at the end

#Calculate commissioning organisation data from GP practice data
checkAggregationCalc <- dataFiltered %>%
  group_by(RegionName, RegionType, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType) %>%
  summarise_at(vars(No..of.eligible.people.on.last.day.of.review.period:X2.5.year.coverage...1), sum, na.rm = T) %>% # adds all numerical values within the groups
  group_by(.drop = "all") %>%
  # calculates the uptake and coverage and formats to the same 1 d.p. as the raw data
  mutate(Uptake.. = as.numeric(format(No..of.people.screened.within.6.months.of.invitation / No..of.people.invited.for.screening.in.previous.12.months * 100, nsmall = 1, digits = 1)), 
         X2.5.year.coverage.. = as.numeric(format(No..of.people.screened.in.previous.30.months / No..of.eligible.people.on.last.day.of.review.period * 100, nsmall = 1, digits = 1)), 
         Uptake...1 = as.numeric(format(No..of.people.screened.within.6.months.of.invitation.1 / No..of.people.invited.for.screening.in.previous.12.months.1 * 100, nsmall = 1, digits = 1)),
         X2.5.year.coverage...1 = as.numeric(format(No..of.people.screened.in.previous.30.months.1 / No..of.eligible.people.on.last.day.of.review.period.1 * 100, nsmall = 1, digits = 1)))

expect_equivalent(checkAggregationRaw, checkAggregationCalc) # checks that the extracted commissioning organisation data matches to the calculated data from the GP practices

errors <- setdiff(checkAggregationCalc, checkAggregationRaw)
errors2 <- setdiff(checkAggregationRaw, checkAggregationCalc)

errors <- bind_rows(errors,errors2)

if(nrow(errors)>0) {
  stop("Check PCT data is aggregated properly")
}

ColNamesAll <- c("RegionName", "RegionType", "CommissioningOrganisation", "CommissioningOrganisationCode", "CommissioningOrganisationType", ColumnNames) #create a list of column name

names(dataFiltered) <- ColNamesAll # rename the columns based on the list

# This section flips the data to long format, splitting the ages into a separate column from the column names, and then flips the values back out
# to the numerical columns. Effectively it reduces the columns by adding the age data to a separate column
dataLong <- dataFiltered %>%
  pivot_longer(cols = c(8:19), names_to = c("Age", "Category"), names_sep = "_") %>%
  pivot_wider(names_from = Category)

# Saves the data as a CSV
write.csv(dataLong, "GPDataWranglingTestV0001.CSV")
               