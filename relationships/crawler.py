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

        
class RelationshipsCrawler(BaseCrawler):
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
        
        # Configure data storage directories
        followsBaseDir = "../../data/relationships/follows"
        followedbyBaseDir = "../../data/relationships/followedby"
        followsDataDir = os.path.join(followsBaseDir, str(resourceID % 1000))
        followedbyDataDir = os.path.join(followedbyBaseDir, str(resourceID % 1000))
        try: 
            os.makedirs(followsDataDir)
            os.makedirs(followedbyDataDir)
        except OSError: pass
        
        # Initialize return variables
        responseCode = 3
        extraInfo = {"InstagramAppFilter": {}}
        
        # ----- Execute collection of follows -----
        followsList = []
        pageCount = 0
        #followsCount = 0
        nextFollowsPage = ""
        while (nextFollowsPage is not None):
            pageCount += 1
            self.echo.out(u"Collecting follows page %d." % pageCount)
            while (True):
                try:
                    follows, nextFollowsPage = api.user_follows(count=100, user_id=resourceID, return_json=True,
                    with_next_url=nextFollowsPage)
                except (InstagramAPIError, InstagramClientError) as error:
                    if (error.status_code == 400):
                        if (error.error_type == "APINotAllowedError"):
                            responseCode = -4
                            responseString = "APINotAllowedError"
                            nextFollowsPage = None
                            break
                        elif (error.error_type == "APINotFoundError"):
                            responseCode = -5
                            responseString = "APINotFoundError"
                            nextFollowsPage = None
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
                    retrys = 0
                    sleepSecondsMultiply = 3
                    if (follows):
                        #followsCount += 1
                        followsList.extend(follows) 
                    break
                    
        # Save JSON file containing the list of follows
        output = open(os.path.join(followsDataDir, "%s.follows" % resourceID), "w")
        json.dump(followsList, output)
        output.close()
        
        # ----- Execute collection of followed by -----
        if (responseCode == 3):
            followedbyList = []
            pageCount = 0
            #followedByCount = 0
            nextFollowedByPage = ""
            while (nextFollowedByPage is not None):
                pageCount += 1
                self.echo.out(u"Collecting followed_by page %d." % pageCount)
                while (True):
                    try:
                        followedby, nextFollowedByPage = api.user_followed_by(count=100, user_id=resourceID, return_json=True, with_next_url=nextFollowedByPage)
                    except Exception as error:
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            self.echo.out(u"API call error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise SystemExit("Maximum number of retrys exceeded.")
                    else:
                        retrys = 0
                        sleepSecondsMultiply = 3
                        if (followedby):
                            #followedByCount += 1
                            followedbyList.extend(followedby) 
                        break
                        
            # Save JSON file containing the list of followed by
            output = open(os.path.join(followedbyDataDir, "%s.followedby" % resourceID), "w")
            json.dump(followedbyList, output)
            output.close()
            
        # Get rate remaining to send back to InstagramAppFilter
        extraInfo["InstagramAppFilter"]["appname"] = application["name"]
        extraInfo["InstagramAppFilter"]["apprate"] = int(api.x_ratelimit_remaining)
            
        return ({#"crawler_name": socket.gethostname(), 
                 "response_code": responseCode}, 
                 #"follows_count": followsCount, 
                 #"followed_by_count": followedByCount}, 
                 extraInfo,
                 None)
                 