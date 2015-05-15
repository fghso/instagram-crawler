# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
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

        
class UsersCrawler(BaseCrawler):
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
        if not os.path.exists(usersDataDir): os.makedirs(usersDataDir)
        
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
        