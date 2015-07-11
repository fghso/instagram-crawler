# -*- coding: iso-8859-1 -*-

import os
#import socket
import json
import time
import stat
import common
#from datetime import datetime
#from datetime import timedelta
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

        
class FeedsCrawlerDB(BaseCrawler):
    def crawl(self, resourceID, filters):      
        # Extract filters
        application = filters[0]["data"]["application"]
        self.echo.out(u"ID: %s (App: %s)." % (resourceID, application["name"]))        
    
        # Get authenticated API object
        clientID = str(application["clientid"])
        clientSecret = str(application["clientsecret"])
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)

        # Configure data storage directory
        feedsBaseDir = "../../data-update/feeds"
        feedsDataDir = os.path.join(feedsBaseDir, str(int(resourceID) % 1000))
        try: os.makedirs(feedsDataDir)
        except OSError: pass
        
        # Configure min and max timestamps
        minTimestamp = 1417392000 # = 01 Dec 2014 00:00:00 UTC
        maxTimestamp = 1435708800 # = 01 Jul 2015 00:00:00 UTC
        
        # Execute collection
        resourceInfo = None
        feedList = []
        pageCount = 0
        nextUserRecentMediaPage = ""
        while (nextUserRecentMediaPage is not None):
            pageCount += 1
            self.echo.out(u"Collecting feed page %d." % pageCount)
            try:
                userRecentMedia, nextUserRecentMediaPage = api.user_recent_media(count=35, user_id=resourceID, return_json=True, with_next_url=nextUserRecentMediaPage, min_timestamp=minTimestamp, max_timestamp=maxTimestamp)
            except (InstagramAPIError, InstagramClientError) as error:
                if (error.status_code == 400): 
                    resourceInfo = {"error": error.error_type}
                    break
                else: raise
            else:
                if (userRecentMedia): feedList.extend(userRecentMedia) 
                
        # Save JSON file containing user feed data
        mediaInfoFilePath = os.path.join(feedsDataDir, "%s.feed" % resourceID)
        with open(mediaInfoFilePath, "w") as output: json.dump(feedList, output)
        os.chmod(mediaInfoFilePath, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
        
        return (resourceInfo, None, None)
                

class FeedsCrawlerFile(BaseCrawler):
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
        feedsBaseDir = "../../data/feeds"
        feedsDataDir = os.path.join(feedsBaseDir, str(resourceID % 1000))
        try: os.makedirs(feedsDataDir)
        except OSError: pass
        
        # Configure minimum media timestamp
        #timeInterval = datetime.utcnow() - timedelta(90)
        #minTimestamp = calendar.timegm(timeInterval.utctimetuple())
        minTimestamp = 1322697600 # = 01 Dec 2011 00:00:00 UTC
        
        # Initialize return variables
        responseCode = 3
        extraInfo = {"InstagramAppFilter": {}}
        #extraInfo = {"InstagramAppFilter": {}, "SaveResourcesFilter": []}
        
        # Execute collection
        feedList = []
        pageCount = 0
        #mediaCount = 0
        nextUserRecentMediaPage = ""
        while (nextUserRecentMediaPage is not None):
            pageCount += 1
            self.echo.out(u"Collecting feed page %d." % pageCount)
            while (True):
                try:
                    userRecentMedia, nextUserRecentMediaPage = api.user_recent_media(count=35, user_id=resourceID, return_json=True, with_next_url=nextUserRecentMediaPage, min_timestamp=minTimestamp)
                except (InstagramAPIError, InstagramClientError) as error:
                    if (error.status_code == 400):
                        if (error.error_type == "APINotAllowedError"):
                            responseCode = -4
                            nextUserRecentMediaPage = None
                            break
                        elif (error.error_type == "APINotFoundError"):
                            responseCode = -5
                            nextUserRecentMediaPage = None
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
                    if (userRecentMedia):
                        #mediaCount += len(userRecentMedia)
                        feedList.extend(userRecentMedia) 
                        
                        # Extract media data to send back to SaveResourcesFilter       
                        # for media in userRecentMedia:
                            # mediaInfo = {"type": media["type"], 
                                         # "url": media["images"]["standard_resolution"]["url"]}
                            # extraInfo["SaveResourcesFilter"].append((media["id"], mediaInfo))
                        
                    break
        
        # Save JSON file containing user complete feed data
        output = open(os.path.join(feedsDataDir, "%s.feed" % resourceID), "w")
        json.dump(feedList, output)
        output.close()
        
        # Get rate remaining to send back to InstagramAppFilter
        extraInfo["InstagramAppFilter"]["appname"] = application["name"]
        extraInfo["InstagramAppFilter"]["apprate"] = int(api.x_ratelimit_remaining)

        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode}, 
                #"media_count": mediaCount},
                extraInfo,
                None)
        