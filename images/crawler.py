# -*- coding: iso-8859-1 -*-

import os
import socket
import json
import time
import urllib2
import common


class BaseCrawler:
    def __init__(self, configurationsDictionary):
        self._extractConfig(configurationsDictionary)
        self.echo = common.EchoHandler(self.config["echo"])
       
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
    
    def crawl(self, resourceID, filters):
        return (None, None, None)
        

class ImagesCrawler(BaseCrawler):
    # Response codes:
    #    3 => Successful collection
    #   -4 => Error in one of the media
    def crawl(self, resourceID, filters):      
        self.echo.out(u"User ID received: %s." % resourceID)
        
        # Configure exception handling
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configure data storage directory
        imagesBaseDir = "../../data/images"
        imagesDataDir = os.path.join(imagesBaseDir, str(resourceID % 1000))
        try: os.makedirs(imagesDataDir)
        except OSError: pass
        
        # Load user feed file
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Initialize return variables
        responseCode = 3
        
        # Execute collection
        for media in feed:
            self.echo.out(u"Media: %s." % media["id"])
            while (True):
                try:
                    header = {"User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
                    request = urllib2.Request(media["images"]["standard_resolution"]["url"], headers = header)
                    #request = urllib2.Request(media["images"]["standard_resolution"]["url"])
                    imageData = urllib2.urlopen(request).read()
                except Exception as error:
                    if type(error) == urllib2.HTTPError:
                        self.echo.out(error, "ERROR")
                        responseCode = -4
                        break
                    else:
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            self.echo.out(u"Request error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                        else:
                            raise SystemExit("Maximum number of retrys exceeded.")
                else:
                    retrys = 0
                    sleepSecondsMultiply = 3
                        
                    # Save image to disk
                    imageFile = "%s.jpg" % media["id"]
                    output = open(os.path.join(imagesDataDir, imageFile), "wb")
                    output.write(imageData)
                    output.close()
                    
                    break
        
        return ({#"crawler_name": socket.gethostname(), 
                "response_code": responseCode}, 
                None,
                None)
        