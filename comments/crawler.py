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

        
class CommentsCrawler(BaseCrawler):
    # Response codes:
    #    3 => Successful collection
    #   -4 => Error in one of the media
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
        commentsBaseDir = "../../data/comments"
        commentsDataDir = os.path.join(commentsBaseDir, str(resourceID % 1000))
        try: os.makedirs(commentsDataDir)
        except OSError: pass
        
        # Load user feed file
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Initialize return variables
        responseCode = 3
        extraInfo = {"InstagramAppFilter": {}}
        
        # Execute collection
        comments = []
        for media in feed:
            self.echo.out(u"Media: %s." % media["id"])
            while (True):
                try:
                    mediaComments = api.media_comments(media_id=media["id"], return_json=True)
                except (InstagramAPIError, InstagramClientError) as error:
                    if (error.status_code == 400):
                        self.echo.out(error, "ERROR")
                        responseCode = -4
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
                    comments.extend(mediaComments)
                    break
        
            # Save JSON file containing comments data for the user feed
            output = open(os.path.join(commentsDataDir, "%s.comments" % media["id"]), "w")
            json.dump(feed, output)
            output.close()
        
        # Get rate remaining to send back to InstagramAppFilter
        extraInfo["InstagramAppFilter"]["appname"] = application["name"]
        extraInfo["InstagramAppFilter"]["apprate"] = int(api.x_ratelimit_remaining) if api.x_ratelimit_remaining else None

        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode}, 
                extraInfo,
                None)
        