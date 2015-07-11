# -*- coding: iso-8859-1 -*-

import os
#import socket
import json
import time
import urllib2
import re
import stat
import common
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from instagram.bind import InstagramClientError


class BaseCrawler:
    def __init__(self, configurationsDictionary):
        self._extractConfig(configurationsDictionary)
        self.echo = common.EchoHandler(self.config["echo"])
       
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
    
    def crawl(self, resourceID, filters):
        return (None, None, None)

        
class UsersCrawlerDB(BaseCrawler):        
    # State codes:
    #   valid => Successful collection
    #   not_allowed => APINotAllowedError - you cannot view this resource
    #   not_found => APINotFoundError - this user does not exist
    def crawl(self, resourceID, filters):
        # Extract filters
        application = filters[0]["data"]["application"]
        self.echo.out(u"ID: %s (App: %s)." % (resourceID, application["name"]))        
        
        # Get authenticated API object
        clientID = str(application["clientid"])
        clientSecret = str(application["clientsecret"])
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)

        # Configure data storage directory
        usersBaseDir = "../../data-update/users"
        usersDataDir = os.path.join(usersBaseDir, str(int(resourceID) % 1000))
        try: os.makedirs(usersDataDir)
        except OSError: pass
        
        # Initialize return variables
        resourceInfo = {"current_state": "valid"}
        extraInfo = {"InstagramAppFilter": {}, "MySQLBatchInsertFilter": []}
        
        # Execute collection
        while True:
            try:
                userInfo = api.user(user_id=resourceID, return_json=True)
                request = urllib2.Request("http://instagram.com/%s" % userInfo["username"])
                userPage = urllib2.urlopen(request).read()
            except (InstagramAPIError, InstagramClientError) as error:
                if (error.status_code == 400):
                    if (error.error_type == "APINotAllowedError"):
                        resourceInfo["current_state"] = "not_allowed"
                        break
                    elif (error.error_type == "APINotFoundError"):
                        resourceInfo["current_state"] = "not_found"
                        break
                else: raise
            else:
                userInfoFilePath = os.path.join(usersDataDir, "%s.user" % resourceID)
                userPageFilePath = os.path.join(usersDataDir, "%s.html" % resourceID)
                with open(userInfoFilePath, "w") as output: json.dump(userInfo, output)
                with open(userPageFilePath, "w") as output: output.write(userPage)
                os.chmod(userInfoFilePath, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
                os.chmod(userPageFilePath, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
                
                # Send user information back to batch insert filter 
                userInfo["counts_media"] = userInfo["counts"]["media"]
                userInfo["counts_follows"] = userInfo["counts"]["follows"]
                userInfo["counts_followed_by"] = userInfo["counts"]["followed_by"]
                del userInfo["counts"]
                extraInfo["MySQLBatchInsertFilter"].append(userInfo)
                
                resourceInfo["is_verified"] = (re.search("\"is_verified\":true", userPage) is not None)
                break

        return (resourceInfo, extraInfo, None)
        
        
class UsersCrawlerFile(BaseCrawler):
    # Response codes:
    #    3 => Successful collection
    #   -4 => APINotAllowedError - you cannot view this resource
    #   -5 => APINotFoundError - this user does not exist
    def crawl(self, resourceID, filters):
        self.echo.out(u"User ID received: %s." % resourceID)
        
        # Extract filters
        application = filters[0]["data"]["application"]
            
        # Get authenticated API object
        clientID = application["clientid"]
        clientSecret = application["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        self.echo.out(u"App: %s." % str(application["name"]))

        # Configure exception handling
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configure data storage directory
        usersBaseDir = "../../data/users"
        usersDataDir = os.path.join(usersBaseDir, str(resourceID % 1000))
        try: os.makedirs(usersDataDir)
        except OSError: pass
        
        # Initialize return variables
        responseCode = 3
        #extraInfo = {"InstagramAppFilter": {}, "SaveResourcesFilter": []}
        extraInfo = {"InstagramAppFilter": {}}
        
        # Execute collection
        while (True):
            try:
                userInfo = api.user(user_id=resourceID, return_json=True)
            except (InstagramAPIError, InstagramClientError) as error:
                if (error.status_code == 400):
                    if (error.error_type == "APINotAllowedError"):
                        responseCode = -4
                        break
                    elif (error.error_type == "APINotFoundError"):
                        responseCode = -5
                        break
                else:
                    if (retrys < maxNumberOfRetrys):
                        sleepSeconds = 2 ** sleepSecondsMultiply
                        self.echo.out(u"API call error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
                        time.sleep(sleepSeconds)
                        sleepSecondsMultiply += 1
                        retrys += 1
                    else:
                        raise SystemExit("Maximum number of retrys exceeded.")
            else:
                output = open(os.path.join(usersDataDir, "%s.user" % resourceID), "w")
                json.dump(userInfo, output)
                output.close()
                
                # Extract user counts to send back to SaveResourcesFilter       
                # userCounts = {"counts_media": userInfo["counts"]["media"], 
                              # "counts_follows": userInfo["counts"]["follows"], 
                              # "counts_followedby": userInfo["counts"]["followed_by"]}
                # extraInfo["SaveResourcesFilter"].append((resourceID, userCounts))
                
                break

        # Get rate remaining to send back to InstagramAppFilter
        extraInfo["InstagramAppFilter"]["appname"] = application["name"]
        extraInfo["InstagramAppFilter"]["apprate"] = int(api.x_ratelimit_remaining)
        
        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode},
                extraInfo,
                None)
        