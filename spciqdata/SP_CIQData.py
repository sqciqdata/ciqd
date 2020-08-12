'''
Created on Aug 9, 2020

@author: wakana_sakashita
'''

if __name__ == '__main__':
    pass

import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta


url = "https://api-ciq.marketintelligence.spglobal.com/gdsapi/rest/v3/clientservice.json"
headers = {'Content-Type': 'application/json'}


class ciq:
    def __init__(self, user, pwd):
        self.user = user
        self.pwd = pwd


    def __call_api(self, json):
    
        resp = requests.post(url, auth=HTTPBasicAuth(self.user, self.pwd), headers=headers, data=json)
    
    
        if(resp.status_code >= 400 and resp.status_code < 600):
            print("HTTP error %d is returned during the API process." %(resp.status_code))
            print(resp.text)
            dat = 'NOT_AVAILABLE'
            return dat

        dat = resp.json()

        if(isinstance(dat, dict) is False):
            print("HTTP error %d is returned during the API process." %(resp.status_code))
            print(resp.text)
            dat = 'NOT_AVAILABLE'
            return dat

        elif('Errors' in dat.keys()):
            print("Error occurred during the API process : %s" %(dat))
            dat = 'NOT_AVAILABlE'
            return dat

        return(dat)

    ## Open,close,high,low,volume data 
    def get_market_data(self, company, num) :

        today = datetime.datetime.today()
        delta = int(num[:len(num)-1])

        if num[-1] == 'y':
            start_day = today - relativedelta(years=delta)
        elif num[-1] == 'm':
            start_day = today - relativedelta(months=delta)
        elif num[-1] == 'd':
            start_day = today - datetime.timedelta(days=delta)

        e_day = today.strftime("%Y-%m-%d")
        s_day = start_day.strftime ('%Y-%m-%d')

        
        json = """{ "inputRequests": [
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_OPENPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_HIGHPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_LOWPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_CLOSEPRICE", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            {"function": "GDST", "identifier": "%s", "mnemonic": "IQ_VOLUME", "properties":{"startDate": "%s", "endDate": "%s", "frequency": "Daily"}},
            ]}""" %(company,s_day,e_day, company,s_day,e_day,company,s_day,e_day,company,s_day,e_day,company,s_day,e_day)
    
        res = self.__call_api(json)
#        print(res)

        SPCIQ = pd.DataFrame()
        for r in res['GDSSDKResponse']:
            if r['NumRows'] > 0:
                if r['Mnemonic'] == 'IQ_OPENPRICE':
                    SPCIQ['Date'] = [pd.to_datetime(x) for x in r['Headers']]
                    SPCIQ['Open'] =  [float(x) for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_HIGHPRICE':
                    SPCIQ['High'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_LOWPRICE':
                    SPCIQ['Low'] = [float(x) for x in r['Rows'][0]['Row']]  
                elif r['Mnemonic'] == 'IQ_CLOSEPRICE':
                    SPCIQ['Close'] = [float(x) for x in r['Rows'][0]['Row']]   
                elif r['Mnemonic'] == 'IQ_VOLUME':
                    SPCIQ['Volume'] = [float(x) for x in r['Rows'][0]['Row']]   
            
        SPCIQ = SPCIQ.set_index('Date')            

        return SPCIQ
