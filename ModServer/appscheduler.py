#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import xmltodict
import httplib2

class CrawlerParams():
    def getParamsDict(self):
        return {"paramkey": "paravalue"}
        
paramsFile = open("params.xml", "r")
params = xmltodict.parse(paramsFile.read())

databaseParams = params["params"]["database"]
applicationParams = params["params"]["instagram"]

#print applicationParams


h = httplib2.Http(disable_ssl_certificate_validation=True)

for i in xrange(0,20):
    (resp_headers, content) = h.request("https://api.instagram.com/v1/tags/nofilter?client_id=9d8098794a73453eb3428b13b94362a7", "GET")
    print resp_headers["x-ratelimit-limit"],
    print "|",
    print resp_headers["x-ratelimit-remaining"]

