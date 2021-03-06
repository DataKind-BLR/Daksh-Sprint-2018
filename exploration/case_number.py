#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jul  1 09:50:57 2018

@author: Swachhy
"""

import pandas as pd
import os
import pylab
from matplotlib import pyplot
import datetime
import seaborn  as sns

case_data=pd.read_excel("data/case_level.xlsx")
hearing_data=pd.read_excel("data/hearing_level.xlsx")

## missing ditricts in Karnataka?
df=case_data[(case_data['court_state']=="Karnataka") & (case_data['district'].isnull())]
df[['court_state','district','case_number']]

##filtering the data to have only Bengaluru districts
case_blr=case_data.loc[case_data['district'].isin(['Bengaluru Urban','BengaluruRural','Bengaluru','Bengaluru City'])]

##the null valued columns are removed from both case and hearing tables
case=case_blr.dropna(how='all',axis='columns')
hearing=hearing_data.dropna(how='all',axis='columns')


caseblr_mod = case[['id','pk_id', 'combined_case_number','case_number','case_type','year','court_name','court_hall_number','date_filed','order_type','petitioner','petitioner_advocate','respondent','respondent_advocate','current_stage','current_status','district','before_honourable_judges','next_hearing_date','filing_number','registration_date','cause_list_date','registration_no','decision_date','nature_of_disposal','petitioner_address','respondent_address','under_sections','under_acts','police_station','court_state','court_type','court_district','stage_of_case','court_complex','court_number','first_hearing_date','created_date']].copy()

##replacing null values in "date_filed" to value in "registration_date"
caseblr_mod['date_filed']=caseblr_mod['date_filed'].fillna(caseblr_mod['registration_date'])

##month from filing date and preprocessing of data
caseblr_mod['file_mon_yr'] = caseblr_mod.date_filed.dt.to_period('M')
caseblr_mod['current_status']=caseblr_mod['current_status'].replace("Disposed","CASE DISPOSED")
caseblr_mod['current_status']=caseblr_mod['current_status'].replace("Pending","CASE PENDING")


## removing the columns that has more than 65% null values
missing= caseblr_mod.isnull().sum()/len(caseblr_mod)
missing_col=missing[missing>0.65].index
caseblr_mod.drop(missing_col,axis=1,inplace=True)


## cases where decision date is null for case disposed
caseblr_mod_temp= caseblr_mod[(caseblr_mod['current_status']=="CASE DISPOSED")& (caseblr_mod['decision_date'].isnull())]
len(caseblr_mod_temp) ## 41  cases have null as decision date
len(caseblr_mod)## 173 cases for dist blr



for i in caseblr_mod['decision_date']:
    if pd.isnull(i):
        caseblr_mod['duration']= datetime.date.today()- (pd.to_datetime(caseblr_mod['date_filed'],format='%d-%m-%Y'))
    else:
        caseblr_mod['duration']=(pd.to_datetime(caseblr_mod['decision_date'],format='%d-%m-%Y'))- (pd.to_datetime(caseblr_mod['date_filed'],format='%d-%m-%Y'))

caseblr_mod['duration_int']=(caseblr_mod['duration'].astype(int))/(1000000000*86400)
caseblr_mod['duration_int']=(caseblr_mod['duration_int']).astype(int)

## classifying the duration values into groups
def duration(caseblr_mod):
    i=0
    a=""
    while i <=len(caseblr_mod):
        if caseblr_mod['duration_int']<1825:
            a= "less than 5 years"
        elif 3650>=caseblr_mod['duration_int']>1825:
            a= "5-10 years"
        elif 5475>=caseblr_mod['duration_int']>3650:
            a= "10-15 years"
        else:
            a= "More than 15 years"

        i=i+1

    return a

caseblr_mod['duration_class']=caseblr_mod.apply(duration,axis=1)
caseblr_mod[['duration','duration_int','duration_class','case_number','date_filed','registration_date','id']].sort_values('duration')


## number of cases per district
plot1=sns.countplot(x="district", data=caseblr_mod)
pyplot.title('number of cases dist-wise')
pylab.show()

##number of cases per current status
plot2=sns.countplot(x="current_status", data=caseblr_mod)
pyplot.title('number of cases status-wise')
pylab.show()

##number of cases per case-type
plot3=sns.countplot(x="case_type", data=caseblr_mod)
pyplot.title('number of cases per case type')
pylab.show()

##distribution of cases per current status and current_stage
plot4=sns.countplot(x="current_status", hue="current_stage", data=caseblr_mod)
pyplot.title('distribution of current status per current stage of case')
pyplot.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
pylab.show()

##seosanility of number of cases
a=caseblr_mod['case_number'].groupby(caseblr_mod['file_mon_yr']).count()
b=caseblr_mod['case_number'].groupby(caseblr_mod['date_filed']).count()
a.plot()
b.plot()

##distribution of cases duration wise per current status
plot5=sns.countplot(x="current_status", hue="duration_class", data=caseblr_mod)
pyplot.title('distribution of duration per current status')
pyplot.legend(bbox_to_anchor=(1.05, 1), loc=2, borderaxespad=0.)
pylab.show()

## number of cases duration wise
plot6=sns.countplot(x="duration_class", data=caseblr_mod)
pyplot.title('number of cases per duration class')
pylab.show()
