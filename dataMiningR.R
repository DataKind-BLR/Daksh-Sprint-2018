setwd("C:/Users/v-mansar/Documents/Daksh")
#setwd("F:/datakind/Daksh/workplace")
library(tidyverse)
library(lubridate)
library(dplyr)
library(readr)
library(reshape2)
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

# Top 5 purpose of hearings in order to understand what are the most frequent types of hearings
# Top 5 ones are
#1. SUMMONS 2. EVIDENCE 3. ARGUMENTS 4. NOTICE 5. Disposed
top_5_hearing_purpose <- hearings %>% group_by(PurposeOfHearing) %>% summarize(Count = n()) 
# Sixth category added will be others
hearings$HearingType <- ifelse(!(hearings$PurposeOfHearing 
                               %in% c('SUMMONS','EVIDENCE','ARGUMENTS','NOTICE','Disposed')),'Others',hearings$PurposeOfHearing)

HearingTypeCounts <- hearings %>% group_by(CaseInformationId,HearingType) %>% summarize(HearingTypeCnt = n())

CaseHearingPurposeCount <- dcast(HearingTypeCounts,CaseInformationId~HearingType,mean)

CaseHearingPurposeCount[,c("SUMMONS", "EVIDENCE","ARGUMENTS","NOTICE","Disposed","Others")] <-
  apply(CaseHearingPurposeCount[,c("SUMMONS", "EVIDENCE","ARGUMENTS","NOTICE","Disposed","Others")], 2, function(x){replace(x, is.na(x), 0)})

CasesFinal <- merge(cases_new,CaseHearingPurposeCount,by.x = "Id",by.y = "CaseInformationId")

#Write to csv
#write_csv(CasesFinal, "CasesChNagar.csv")


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
CasesFinal <- CasesFinal %>% 
  left_join(crnagarCasesOpen,by = c("Id" = "CaseInformationId")) %>%
  left_join(crnagarCasesClosed, by = c("Id" = "CaseInformationId"))

# Decision date where max date is still unknown
CasesFinal[CasesFinal$CurrentStatus == "Pending" &is.na(CasesFinal$MaxDate), ]$MaxDate <- Sys.Date()
CasesFinal[is.na(CasesFinal$MaxDate),]$MaxDate <- CasesFinal[is.na(CasesFinal$MaxDate),]$DecisionDate



# Decision date where min date is still unknown 
CasesFinal[is.na(CasesFinal$MinDate),]$MinDate <- CasesFinal[is.na(CasesFinal$MinDate),]$RegistrationDate


# Time taken variable addition
CasesFinal$CaseLength <- as.Date(as.character(CasesFinal$MaxDate), format="%Y-%m-%d") - as.Date(as.character(CasesFinal$MinDate), format="%Y-%m-%d")

#CasesFinal$DecDateisMax <- CasesFinal$MaxDate == CasesFinal$DecisionDate

#cases_new %>% group_by(CurrentStatus, DecDateisMax) %>%
#  summarise(number_of_decismax = n(),
#            meanTimeTaken = mean(timeTaken))


#Write to csv
write_csv(CasesFinal, "CasesChNagar.csv")
