setwd("F:/datakind/Daksh/workplace")
library(tidyverse)
library(lubridate)
#library()

#Read DAta 
crnagarCases <- read_csv("senior_civil_judge_and_cjm_court,_chamarajanagar_cases_processed.csv")
crnagarHearing <- read_csv("senior_civil_judge_and_cjm_court,_chamarajanagar_hearings_processed.csv")


## Manav's Code

# Data load
hearings <- crnagarHearing
cases <- crnagarCases

#hearing_subset = subset(hearings,CaseInformationId == "9c83a4f3-2f80-4ded-82fe-d042c42513e6")

#Dropping unnecessary column
hearings<- hearings[,!(names(hearings) == "X1")]
cases<- cases[,!(names(cases) == "X1")]

#changing column Id to hearinguniqueId
colnames(hearings)[colnames(hearings) == 'Id'] <- 'HearingId'

#merging the cases and their respective hearings
# number of cases after merge = 4,877
cases_hearings <- merge(cases,hearings,by.x = "Id", by.y = "CaseInformationId")

#calculating difference between business data and hearing date

cases_hearings$DateDiff <- as.Date(as.character(cases_hearings$HearingDate), format="%Y-%m-%d")-
  as.Date(as.character(cases_hearings$BusinessOnDate), format="%Y-%m-%d")


time_between_hearings <- cases_hearings %>% group_by(Id) %>%
  summarize(TimeBetweenHearings = round(mean(DateDiff,na.rm=T)),
            NumberOfHearings = n_distinct(HearingId))


cases_new <- left_join(cases,time_between_hearings,by = "Id")

cases_new$TimeForFirstHearing <- as.Date(as.character(cases_new$FirstHearingDate), format="%Y-%m-%d")-
  as.Date(as.character(cases_new$RegistrationDate), format="%Y-%m-%d")

cases_new$PetitionerDeadAlive <- ifelse(grepl("DEAD",cases_new$Petitioner,ignore.case = T)==TRUE,"Dead","Alive")

cases_new$RespondentDeadAlive <- ifelse(grepl("DEAD",cases_new$Respondent,ignore.case = T)==TRUE,"Dead","Alive")

cases_new$PetitionerPubPvtInd <- ifelse(grepl("OFFICER|BANK|COMMISSIONER|LAO|SPL|ACQUISITION|TRANSPORT|SPECIAL|S.B.M.|DIVISIONAL|STATE",cases_new$Petitioner,ignore.case = T)==TRUE,"Public",
                                  ifelse(grepl("INSURANCE|PRESIDENT|MANGER|MANAGER|COMPANY|INSURENCE",cases_new$Petitioner,ignore.case = T)==TRUE,"Private","Individual"))

cases_new$RespondentPubPvtInd <- ifelse(grepl("OFFICER|BANK|COMMISSIONER|LAO|SPL|ACQUISITION|TRANSPORT|SPECIAL|S.B.M.|DIVISIONAL|STATE",cases_new$Respondent,ignore.case = T)==TRUE,"Public",
                                  ifelse(grepl("INSURANCE|PRESIDENT|MANGER|MANAGER|COMPANY|INSURENCE",cases_new$Respondent,ignore.case = T)==TRUE,"Private","Individual"))

##Sankarshan's Code
# MergeData
crnagarData <- left_join(crnagarHearing, crnagarCases, 
                         by = c("CaseInformationId" = "Id"))

# Date of case closing -for disposed cases only
crnagarCasesClosed <- crnagarData %>% 
  filter(CurrentStatus == "Disposed") %>% 
  group_by(CaseInformationId) %>% 
  summarise(MaxBusinessOnDate = max(BusinessOnDate, na.rm = T),
            MaxHearingDate = max(HearingDate, na.rm = T),
            MaxDecisionDate = max(DecisionDate, na.rm = T)) %>% 
  gather(key, Dates, -CaseInformationId) %>%
  group_by(CaseInformationId) %>% 
  summarise(MaxDate = max(Dates, na.rm = T))

# Date of case opening - for all case types - disposed & pending
crnagarCasesOpen <- crnagarData %>%
  group_by(CaseInformationId) %>% 
  summarise(RegDate = min(RegistrationDate, na.rm = T),
            FirstHearingDate = min(FirstHearingDate, na.rm = T),
            MinHearingDate = min(HearingDate, na.rm = T)) %>% 
  gather(key, Dates, -CaseInformationId) %>%
  group_by(CaseInformationId) %>% 
  summarise(MinDate = min(Dates, na.rm = T))

# Merging opening and closing dates
cases_new <- cases_new %>% 
  left_join(crnagarCasesOpen,by = c("Id" = "CaseInformationId")) %>%
  left_join(crnagarCasesClosed, by = c("Id" = "CaseInformationId"))

# Decision date where max date is still unknown
cases_new[cases_new$CurrentStatus == "Pending" &is.na(cases_new$MaxDate), ]$MaxDate <- Sys.Date()
cases_new[is.na(cases_new$MaxDate),]$MaxDate <- cases_new[is.na(cases_new$MaxDate),]$DecisionDate



# Decision date where min date is still unknown 
cases_new[is.na(cases_new$MinDate),]$MinDate <- cases_new[is.na(cases_new$MinDate),]$RegistrationDate


# Time taken variable addition
cases_new$timeTaken <- cases_new$MaxDate - cases_new$MinDate

cases_new$DecDateisMax <- cases_new$MaxDate == cases_new$DecisionDate

cases_new %>% group_by(CurrentStatus, DecDateisMax) %>%
  summarise(number_of_decismax = n(),
            meanTimeTaken = mean(timeTaken))


#Write to csv
write_csv(cases_new, "cases_new.csv")
