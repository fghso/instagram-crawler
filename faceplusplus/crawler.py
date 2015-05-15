# -*- coding: iso-8859-1 -*-

import os
import json
import random
import common
from fpp.facepp import API
from fpp.facepp import APIError


class BaseCrawler:
    def __init__(self, configurationsDictionary):
        self._extractConfig(configurationsDictionary)
        self.echo = common.EchoHandler(self.config["echo"])
       
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
    
    def crawl(self, resourceID, filters):
        return (None, None, None)


class FPPURLCrawler(BaseCrawler):
    def crawl(self, resourceID, filters):
        # Configure data storage directory
        #fppBaseDir = "../../data/fpp"
        fppBaseDir = "../../data/fppselfies"
        fppSubDir = str(int(resourceID.split("_")[0]) % 1000) # OBS: The division here group the media by media ID, instead of group by user ID. To group by user ID it is necessary to change to resourceID.split("_")[1].
        fppDataDir = os.path.join(fppBaseDir, fppSubDir)
        if not os.path.exists(fppDataDir): os.makedirs(fppDataDir)
        
        # Initialize return variables
        #extraInfo = {"mediaerrors": []}
        extraInfo = {"mediaerrors": [], "output": []}
        
        # Check if the file already exists
        fppFilePath = os.path.join(fppDataDir, "%s.json" % resourceID)
        if os.path.isfile(fppFilePath): 
            self.echo.out(u"Media %s already exists." % resourceID)
            return (None, extraInfo, None)
        
        # Extract filters
        imageURL = filters[0]["data"]["url"]
        application = filters[1]["data"]["application"]
    
        # Get authenticated API object
        apiServer = application["apiserver"]
        apiKey = application["apikey"]
        apiSecret = application["apisecret"]
        api = API(srv = apiServer, key = apiKey, secret = apiSecret, timeout = 60, max_retries = 0, retry_delay = 0)
        self.echo.out(u"ID: %s (App: %s)." % (resourceID, application["name"]))
        
        # Execute collection
        attributes = ["gender", "age", "race", "smiling", "glass", "pose"]
        try:
            response = api.detection.detect(url = imageURL, attribute = attributes)
        except Exception as error: 
            # HTTP error codes: http://www.faceplusplus.com/detection_detect/
            if isinstance(error, APIError): message = "%d: %s" % (error.code, json.loads(error.body)["error"])
            # socket.error and urllib2.URLError 
            else: message = str(error)
            extraInfo["mediaerrors"].append((resourceID, {"error": message}))
        else: 
            with open(fppFilePath, "w") as fppFile: json.dump(response, fppFile)
            
            for face in response["face"]:
                faceInfo = filters[0]["data"]
                faceInfo["gender_val"] = face["attribute"]["gender"]["value"]
                faceInfo["gender_cnf"] = face["attribute"]["gender"]["confidence"]
                faceInfo["race_val"] = face["attribute"]["race"]["value"]
                faceInfo["race_cnf"] = face["attribute"]["race"]["confidence"]
                faceInfo["smile"] = face["attribute"]["smiling"]["value"]
                faceInfo["age_val"] = face["attribute"]["age"]["value"]
                faceInfo["age_rng"] = face["attribute"]["age"]["range"]
                extraInfo["output"].append((resourceID, faceInfo))
        
        return (None, extraInfo, None)

        
class FPPRandomMediaCrawler(BaseCrawler):  
    def crawl(self, resourceID, filters):
        self.echo.out(u"User ID received: %s." % resourceID)
        
        # Extract filters
        application = filters[0]["data"]["application"]
    
        # Get authenticated API object
        apiServer = application["apiserver"]
        apiKey = application["apikey"]
        apiSecret = application["apisecret"]
        api = API(srv = apiServer, key = apiKey, secret = apiSecret, timeout = 5, max_retries = 0, retry_delay = 0)
        self.echo.out(u"App: %s." % str(application["name"]))
    
        # Configure data storage directory
        fppBaseDir = "../../data/fpp"
        fppDataDir = os.path.join(fppBaseDir, str(resourceID % 1000), str(resourceID))
        if not os.path.exists(fppDataDir): os.makedirs(fppDataDir)
        
        # Load user feed file
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Get random media sample to collect
        feedSampleSize = 10
        feedList = random.sample(feed, min(len(feed), feedSampleSize))
        
        # Initialize return variables
        extraInfo = {"mediaerrors": [], "usersok": [], "usersproblem": []}
        
        # Execute collection
        attributes = ["gender", "age", "race", "smiling", "glass", "pose"]
        for i, media in enumerate(feedList):
            self.echo.out(u"Collecting media %d." % (i + 1))
            try:
                response = api.detection.detect(url = media["images"]["low_resolution"]["url"], attribute = attributes)
            except Exception as error: 
                # HTTP error codes: http://www.faceplusplus.com/detection_detect/
                if isinstance(error, APIError): message = "%d: %s" % (error.code, json.loads(error.body)["error"])
                # socket.error and urllib2.URLError 
                else: message = str(error)
                extraInfo["mediaerrors"].append((media["id"], {"error": message}))
                extraInfo["usersproblem"].append((resourceID, None))
            else:
                fppFilePath = os.path.join(fppDataDir, "%s.faces" % media["id"])
                with open(fppFilePath, "w") as fppFile: json.dump(response, fppFile)
                    
        extraInfo["usersok"].append((resourceID, None))
        return (None, extraInfo, None)
      