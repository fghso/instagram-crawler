# -*- coding: iso-8859-1 -*-

import os
import json
import random
import stat
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
        
        
class FPPCrawlerDB(BaseCrawler):
    def crawl(self, resourceID, filters):      
        # Configure FPP file path
        #fppBaseDir = "../../data-update/fpp"
        fppBaseDir = "../../data-cosn/fpp"
        fppHashID = resourceID.split("_")
        if len(fppHashID) > 1: fppSubDir = os.path.join("media", str(int(fppHashID[1]) % 1000)) # Feed picture
        else: fppSubDir = os.path.join("profiles", str(int(fppHashID[0]) % 1000)) # Profile picture
        fppDataDir = os.path.join(fppBaseDir, fppSubDir)
        try: os.makedirs(fppDataDir)
        except OSError: pass
        fppFilePath = os.path.join(fppDataDir, "%s.fpp" % resourceID)
        
        # Extract filters
        if len(fppHashID) > 1: 
            mediaPK = filters[0]["data"]["media_pk"]
            imageURL = filters[0]["data"]["std_res_url"]
        else: 
            fromTable = filters[0]["data"]["from_table"]
            imageURL = filters[0]["data"]["profile_picture"]
        application = filters[1]["data"]["application"]
        
        # Initialize return variable
        extraInfo = {"MySQLBatchInsertFilter": []}
        
        # Get FPP response
        if os.path.isfile(fppFilePath): 
            self.echo.out(u"Media %s already exists." % resourceID)
            with open(fppFilePath, "r") as fppFile: response = json.load(fppFile)
        else:
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
                resourceInfo = {"faces_count": -1, "error": message}
                return (resourceInfo, extraInfo, None)
            else: 
                with open(fppFilePath, "w") as fppFile: json.dump(response, fppFile)
                os.chmod(fppFilePath, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
            
        # Send faces information back to batch insert filter
        for face in response["face"]:
            if len(fppHashID) > 1: faceInfo = {"media_pk_ref": mediaPK}
            else: faceInfo = {"user_id": resourceID, "from_table": fromTable}
            faceInfo["gender_value"] = face["attribute"]["gender"]["value"]
            faceInfo["gender_confidence"] = face["attribute"]["gender"]["confidence"]
            faceInfo["age_value"] = face["attribute"]["age"]["value"]
            faceInfo["age_range"] = face["attribute"]["age"]["range"]
            faceInfo["smiling_value"] = face["attribute"]["smiling"]["value"]
            extraInfo["MySQLBatchInsertFilter"].append(faceInfo)
        
        resourceInfo = {"faces_count": len(response["face"]), "error": None}
        return (resourceInfo, extraInfo, None)        
        
        
class FPPCrawlerFile(BaseCrawler):
    def crawl(self, resourceID, filters):
        # Configure FPP file path
        fppBaseDir = "../../data-cosn/fpp"
        fppHashID = resourceID.split("_")
        if len(fppHashID) > 1: fppSubDir = os.path.join("media", str(int(fppHashID[1]) % 1000)) # Feed picture
        else: fppSubDir = os.path.join("profiles", str(int(fppHashID[0]) % 1000)) # Profile picture
        fppDataDir = os.path.join(fppBaseDir, fppSubDir)
        try: os.makedirs(fppDataDir)
        except OSError: pass
        fppFilePath = os.path.join(fppDataDir, "%s.fpp" % resourceID)
        
        # Initialize return variables
        extraInfo = {"MediaErrorsFilter": [], "OutputFilter": []}
        
        # Check if the file already exists
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
            extraInfo["MediaErrorsFilter"].append((resourceID, {"error": message}))
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
                extraInfo["OutputFilter"].append((resourceID, faceInfo))
        
        return (None, extraInfo, None)        
