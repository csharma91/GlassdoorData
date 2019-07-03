# -*- coding: utf-8 -*-
"""
Created on Tue Jul  2 13:00:48 2019

@author: csharma
"""

import pyodbc
import pandas as pd
import requests
import json

import time



def GetDataFromSQL():
    SERVER = 'symalpha.canadacentral.cloudapp.azure.com'
    DATABASE = 'SymAlpha'
    USERNAME = 'research'
    PASSWORD = 'SymAlphaLab***'
    
    DRIVER= '{ODBC Driver 13 for SQL Server};'
    cnxn = pyodbc.connect('DRIVER=' + DRIVER + ';PORT=1433;SERVER=' + SERVER +
        ';PORT=1443;DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD)
    
    sql = '''
    
      select distinct companyid,SPCompanyName, employerid, companyname from SymAlpha.dbo.tbGlassDoorCompanyIdMap
    '''        
    df = pd.read_sql(sql = sql, con = cnxn)
    
    cnxn.close()
    return df



datalist = GetDataFromSQL()

gdemployerid = datalist['employerid']
spcompanyid = datalist ['companyid']
spcompanyname = datalist['SPCompanyName']
gdcompanyname = datalist ['companyname']



userid  = '62624'
userkey = 'de2QRGh2fKM'
#query = 'ConocoPhillips'
counter = 0

xpressfeedcompanyname,employerid, companyname, companywebsite,exactmatch,numofratings, overallrating,bizoutlook,ratingdesc, cultureandvalue, seniorleadership,compensationAndBenefits, careerOpportunities \
,workLifeBalance,recommendToFriend, ceoname, ceotitle,ceonumberofratings, ceopctapprove, sectorid, sectorname, industryid, industryname = ([] for i in range (23))


spcompanyid_final = []
gdcompanyid_final = []


    
for emp, spcompanyid in  zip(gdemployerid,spcompanyid):
    
    counter += 1
    
    gdcompanyid_final.append(emp)
    spcompanyid_final.append(spcompanyid)
   
    try:
        #url = 'http://api.glassdoor.com/api/api.htm?v=1&format=json&t.p=62624&t.k=de2QRGh2fKM&employer='+str(emp)+'&action=employers&pn=2&ps=1000&useragent=Mozilla/%2F4.0/'
        url = 'https://www.glassdoor.com/api/employer/'+ str(emp) +'-rating.htm?'
        headers = {'user-agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        output = json.loads(response.text)
    except:
        pass      
    
    
    try:
        overallrating.append(output['ratings'][0]['value'])
    except:
        overallrating.append('')
        
        
    try:
        ceopctapprove.append(output['ratings'][1]['value'])
    except:
        ceopctapprove.append('')
        
        
    try:
        bizoutlook.append(output['ratings'][2]['value'])
    except:
        bizoutlook.append('')
               
    
    try:
        recommendToFriend.append(output['ratings'][3]['value'])
    except:
        recommendToFriend.append('')
        
        
    try:
        compensationAndBenefits.append(output['ratings'][4]['value'])
    except:
        compensationAndBenefits.append('')

        
    try:
        cultureandvalue.append(output['ratings'][5]['value'])
    except:
         cultureandvalue.append('')


    try:
        careerOpportunities.append(output['ratings'][6]['value'])
    except:
        careerOpportunities.append('')
        

        
    try:
        workLifeBalance.append(output['ratings'][7]['value'])
    except:
        workLifeBalance.append('')
        
        
        
    try:
        seniorleadership.append(output['ratings'][8]['value'])
    except:
        seniorleadership.append('')
        
        
    try:
        url = 'http://api.glassdoor.com/api/api.htm?&format=json&t.p=62624&t.k=de2QRGh2fKM&employer='+ str(emp) + '&action=employers&useragent=Mozilla/%2F4.0/'
        #url = 'https://www.glassdoor.com/api/employer/'+ str(emp) +'-rating.htm?'
        headers = {'user-agent': 'Mozilla/5.0'}
        response2 = requests.get(url, headers=headers)
        output2 = json.loads(response2.text)
        
    except:
        pass      
                        
        
    try:
        numofratings.append(output2['response']['employers'][0]['numberOfRatings'])
    except:
        numofratings.append('')
    
    try:    
        companyname.append(output2['response']['employers'][0]['name'])
    except:
        companyname.append('')
        
    try:    
        companywebsite.append(output2['response']['employers'][0]['website'])
    except:
        companywebsite.append('')

    try:
        ceonumberofratings.append(output2['response']['employers'][0]['ceo']['numberOfRatings'])
    except:
        ceonumberofratings.append('')


    
    print (counter)
    print(emp)
    print(spcompanyid)
    #print(output2['response']['employers'][0]['name'])
    #print(output2['response']['employers'][0]['website'])
     
               
    if (counter%10 == 0 ):    
    
        data_tuples = list(zip(spcompanyid_final,gdcompanyid_final,overallrating,ceopctapprove,bizoutlook,recommendToFriend,compensationAndBenefits,cultureandvalue,careerOpportunities,workLifeBalance,seniorleadership,numofratings,ceonumberofratings,companyname,companywebsite))
            
        df_final = pd.DataFrame(data_tuples, columns=['spcompanyid','gdemployerid','OverallRating','CEORating','BizOutlook','Recommend','CompAndBenefits','CultureAndValues','CareerOppurtunities','WorkLife','SeniorManagement','NumOfRatings','CEONumOfRating','GDCompanyName','GDCompanyWebsite'])        
    
        df_final.to_csv('GlassdoorDataPull_20190703.csv')
        print('Create Copy')
        
    if (counter%300 == 0 ):
        time.sleep(10)
    
    
def UploadToSQL(df_final):

    SERVER = 'symalpha.canadacentral.cloudapp.azure.com'
    DATABASE = 'SymAlpha'
    USERNAME = 'research'
    PASSWORD = 'SymAlphaLab***'
    
    DRIVER= '{ODBC Driver 13 for SQL Server};'
    cnxn = pyodbc.connect('DRIVER=' + DRIVER + ';PORT=1433;SERVER=' + SERVER +
        ';PORT=1443;DATABASE=' + DATABASE + ';UID=' + USERNAME + ';PWD=' + PASSWORD)
    
    
    cursor = cnxn.cursor()
    
    for index,row in df_final.iterrows():
        cursor.execute("INSERT INTO [dbo].[GlassdoorDataPull_20190702_4]([spcompanyid],[gdemployerid],[OverallRating],[CEORating],[BizOutlook],[Recommend],[CompAndBenefits],[CultureAndValues],[CareerOppurtunities],[WorkLife],[SeniorManagement],[NumOfRatings],[CEONumOfRating],[GDCompanyName],[GDCompanyWebsite]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", row['spcompanyid'], row['gdemployerid'], row['OverallRating'], row['CEORating'], row['BizOutlook'], row['Recommend'], row['CompAndBenefits'], row['CultureAndValues'],row['CareerOppurtunities'], row['WorkLife'], row['SeniorManagement'], row['NumOfRatings'], row ['CEONumOfRating'], row ['GDCompanyName'], row ['GDCompanyWebsite']) 
        cnxn.commit()
    
    cursor.close()
    cnxn.close()

