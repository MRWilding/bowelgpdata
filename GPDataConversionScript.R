# Load packages using groundhog package
groundhogDay <- "2021-10-01"

library(groundhog)

groundhog.library(c("dplyr", "tidyr", "testthat"), groundhogDay)

# Set column names for output from file
ColumnNames <- c("Organisation", "OrganisationCode", "60to69_eligible-on-last-day", 
                 "60to69_invited-to-screening-previous-12-months", "60to69_screened-within-6-months-of-invite", 
                 "60to69_screened-within-previous-30-months", "60to69_Uptake", "60to69_2.5-year-coverage", "60to74_eligible-on-last-day", 
                 "60to74_invited-to-screening-previous-12-months", "60to74_screened-within-6-months-of-invite", 
                 "60to74_screened-within-previous-30-months", "60to74_Uptake", "60to74_2.5-year-coverage") 

#defines the path to use for data extraction
path <- "W:\\DataAndInfo_NDT\\NationalDataTeam_Restricted\\OpenExeter\\Bowel\\GPPP_2021Oct\\Year_2010\\css_csv_export_bcs_report_Apr10.csv"

#reads data into the environment for the path, skipping the first 3 rows of the CSV to get column headings - converting blank cells to NA
test <- read.csv(path, skip = 3, na.strings = c("NA",""))

test2 <- test %>%
  mutate(RegionType = names(test[1])) %>%
  rename(RegionName = names(test[1])) %>%
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
  select(RegionName, RegionType, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType, everything()) %>% #reorders the data frame to move the new column to the organisation columns
  fill(RegionName, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType, .direction = "down") %>% # fill the data for the region and commissioning organisation down 
  filter(!is.na(Organisation)) # remove the rows where there is no Organisation (Removes the blank region rows)
  
checkAggregationRaw <- test2 %>%
  filter(CommissioningOrganisation == Organisation) %>%
  select(-Organisation, -Organisation.Code)

test3 <- test2 %>% filter(CommissioningOrganisation != Organisation) %>% #Remove the rows for the commissioning organisation (can aggregate up from GP data)
  select(-ncol(test2)) #drop the empty row at the end

checkAggregationCalc <- test3 %>%
  group_by(RegionName, RegionType, CommissioningOrganisation, CommissioningOrganisationCode, CommissioningOrganisationType) %>%
  summarise_at(vars(No..of.eligible.people.on.last.day.of.review.period:X2.5.year.coverage...1), sum, na.rm = T) %>%
  group_by(.drop = "all")

expect_equivalent(checkAggregationRaw, checkAggregationCalc)



ColNamesAll <- c("RegionName", "RegionType", "CommissioningOrganisation", "CommissioningOrganisationCode", "CommissioningOrganisationType", ColumnNames) #create a list of column name

names(test3) <- ColNamesAll # rename the columns based on the list

# This section flips the data to long format, splitting the ages into a separate column from the column names, and then flips the values back out
# to the numerical columns. Effectively it reduces the columns by adding the age data to a separate column
test4 <- test3 %>%
  pivot_longer(cols = c(5:16), names_to = c("Age", "Category"), names_sep = "_") %>%
  pivot_wider(names_from = Category)

# Saves the data as a CSV
write.csv(test3, "GPDataWranglingTestV0001.CSV")
               