'''
Created on Aug 10, 2020

@author: wakana_sakashita
'''
from spciqdata import SP_CIQData

ciq = SP_CIQData.ciq("wakana.sakashita@spglobal.com", "HakubaSki!21")
SP = ciq.get_market_data("AAPL:", '5y')

print(SP)
