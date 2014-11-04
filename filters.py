# -*- coding: iso-8859-1 -*-

import sys
import os
import xmltodict
import httplib2
import time
import logging
import random
from collections import OrderedDict


# The filters are sequentially applied in the same order in wich they were specified in the 
# configuration file, unless they were explicitly set as parallel in the configuration 
class BaseFilter(): 
    def __init__(self, name):
        if (name): self.name = name
        else: self.name = self.__class__.__name__
    
    def getName(self):
        return self.name
 
    # Apply must return a dictionary containing the desired filter information to be sent for the client. 
    # The value of previousFilterData will always be None if the filter is executed in parallel
    def apply(self, resourceID, resourceInfo, previousFilterData):
        return resourceInfo


class InstagramAppFilter(BaseFilter):
    def apply(self, resourceID, resourceInfo, previousFilterData):
        self._loadAppFile()
        while (True):
            (bestRate, appIndex) = self._getBestRate()
            if (bestRate == 0): time.sleep(300)
            else: break
        return OrderedDict([("application", self.applicationList[appIndex])])
        
    def _loadAppFile(self):
        if os.path.exists(os.path.join(sys.path[0], "app.xml")): 
            appFile = open(os.path.join(sys.path[0], "app.xml"), "r")
        else: 
            appFile = open(os.path.join(sys.path[1], "app.xml"), "r")
        appDict = xmltodict.parse(appFile.read())
        self.applicationList = appDict["instagram"]["application"] if isinstance(appDict["instagram"]["application"], list) else [appDict["instagram"]["application"]]
        appFile.close()

    def _getBestRate(self):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        rateList = []
        for application in self.applicationList:
            clientID = application["clientid"]
            rateRemaining = 0
            try: 
                (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
                rateRemaining = int(header["x-ratelimit-remaining"])
            except:
                if (header['status'] == '503') or (header['status'] == '429'):
                    logging.warning("Ratelimit de %s esgotado." % application["@name"])
                else:
                    message = "Falha na requisicao para verificar ratelimit de %s.\n" % application["@name"]
                    message += "Header: %s\n" % header
                    message += "Content: %s\n" % content
                    logging.error(message)
            rateList.append(rateRemaining)
        maxRate = max(rateList)
        return (maxRate, rateList.index(maxRate))
        
    # Funções apenas para testes
    def _spendRandomRate(self, repeat):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        for i in range(1, repeat + 1):
            appIndex = random.randint(0, len(self.applicationList) - 1)
            clientID = self.applicationList[appIndex]["clientid"]
            (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.applicationList[appIndex]["@name"], header["x-ratelimit-remaining"]
            
    def _spendSpecificRate(self, appIndex, repeat):
        httpObj = httplib2.Http(disable_ssl_certificate_validation=True)
        for i in range(1, repeat + 1):
            clientID = self.applicationList[appIndex]["clientid"]
            (header, content) = httpObj.request("https://api.instagram.com/v1/tags/selfie?client_id=%s" % clientID, "GET")
            print "%d: " % i, self.applicationList[appIndex]["@name"], header["x-ratelimit-remaining"]#, content 

# ====== Testes ===== 
#print InstagramAppFilter()._spendRandomRate(50)
#print InstagramAppFilter()._spendSpecificRate(18, 1)
#print InstagramAppFilter("").apply(None, None, None)
