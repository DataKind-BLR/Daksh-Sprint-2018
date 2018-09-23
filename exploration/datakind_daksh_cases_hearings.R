# Title : Daksh - DataKind-BLR
# Created : 19, August 2018
# Author : Manav

# Packages
library(dplyr)
library(readr)
library(lubridate)

# Directory
setwd("C:/Users/v-mansar/Downloads")
dir()

# Data load
hearings <- read_csv(file = 'senior_civil_judge_and_cjm_court,_chamarajanagar_hearings_processed.csv',col_names = TRUE,na = c("", "NA"))
cases <- read_csv(file = 'senior_civil_judge_and_cjm_court,_chamarajanagar_cases_processed.csv',col_names = TRUE,na = c("", "NA"))

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


cases_new <- merge(cases,time_between_hearings,by = "Id")

cases_new$TimeForFirstHearing <- as.Date(as.character(cases_new$FirstHearingDate), format="%Y-%m-%d")-
  as.Date(as.character(cases_new$RegistrationDate), format="%Y-%m-%d")

cases_new$PetitionerDeadAlive <- ifelse(grepl("DEAD",cases_new$Petitioner,ignore.case = T)==TRUE,"Dead","Alive")

cases_new$RespondentDeadAlive <- ifelse(grepl("DEAD",cases_new$Respondent,ignore.case = T)==TRUE,"Dead","Alive")

cases_new$PetitionerPIP <- ifelse(grepl("OFFICER|BANK|COMMISSIONER|LAO|SPL|ACQUISITION|TRANSPORT|SPECIAL|S.B.M.|DIVISIONAL|STATE",cases_new$Petitioner,ignore.case = T)==TRUE,"Public",
                                  ifelse(grepl("INSURANCE|PRESIDENT|MANGER|MANAGER|COMPANY|INSURENCE",cases_new$Petitioner,ignore.case = T)==TRUE,"Private","Individual"))

cases_new$RespondentPIP <- ifelse(grepl("OFFICER|BANK|COMMISSIONER|LAO|SPL|ACQUISITION|TRANSPORT|SPECIAL|S.B.M.|DIVISIONAL|STATE",cases_new$Respondent,ignore.case = T)==TRUE,"Public",
                                  ifelse(grepl("INSURANCE|PRESIDENT|MANGER|MANAGER|COMPANY|INSURENCE",cases_new$Respondent,ignore.case = T)==TRUE,"Private","Individual"))

write.csv(cases_new,file = "Cases_District.csv",col.names = T,row.names = F)


chk <- cases %>% group_by(NatureOfDisposal) %>% summarize(cnt = n())
  
chk2<- cases[is.na(cases$DecisionDate),]    
