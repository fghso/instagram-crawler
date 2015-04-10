# -*- coding: iso-8859-1 -*-

"""Module to store crawler classes.

More than one class can be written here, but only one (that specified in the configuration file) will be used by the client to instantiate a crawler object whose :meth:`crawl() <BaseCrawler.crawl>` method will by called to do the collection of the resource received. 

"""

import os
import socket
import json
import time
import urllib2
import common


class BaseCrawler:
    """Abstract class. All crawlers should inherit from it or from other class that inherits."""

    def __init__(self, configurationsDictionary):
        """Constructor.  
        
        Upon initialization the crawler object receives everything in the crawler section of the XML configuration file as the parameter *configurationsDictionary*. 
        
        """
        self._extractConfig(configurationsDictionary)
       
    def _extractConfig(self, configurationsDictionary):
        """Extract and store configurations.
        
        If some configuration needs any kind of pre-processing, it is done here. Extend this method if you need to pre-process custom configuration options.
        
        """
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
    
    def crawl(self, resourceID, filters):
        """Collect the resource.
        
        Must be overriden.
        
        Args:
            * *resourceID* (user defined type): ID of the resource to be collected, sent by the server.
            * *filters* (list): All data (if any) generated by the filters added to server. Sequential filters data come first, in the same order that the filters were specified in the configuration file. Parallel filters data come next, in undetermined order.
            
        Returns:   
            A tuple in the format (*resourceInfo*, *extraInfo*, *newResources*). Any element of the tuple can be ``None``, depending on what the user desires.

            * *resourceInfo* (dict): Resource information dictionary. This information is user defined and must be understood by the persistence handler used. 
            * *extraInfo* (dict): Aditional information. This information is just passed to all filters via :meth:`callback() <filters.BaseFilter.callback>` method and is not used by the server itself. 
            * *newResources* (list): Resources to be stored by the server when the feedback option is enabled. Each new resource is described by a tuple in the format (*resourceID*, *resourceInfo*), where the first element is the resource ID (whose type is defined by the user) and the second element is a dictionary containing resource information (in a format understood by the persistence handler used).
                
        """
        return (None, None, None)
        

class ImagesCrawler(BaseCrawler):
    # Response codes:
    #    3 => Successful collection
    #   -4 => Error in one of the media
    def crawl(self, resourceID, filters):      
        echo = common.EchoHandler(self.config)
        echo.out(u"User ID received: %s." % resourceID)
        
        # Configure exception handling
        maxNumberOfRetrys = 8
        retrys = 0
        sleepSecondsMultiply = 3
        
        # Configure data storage directory
        imagesBaseDir = "../../data/images"
        imagesDataDir = os.path.join(imagesBaseDir, str(resourceID % 1000))
        if not os.path.exists(imagesDataDir): os.makedirs(imagesDataDir)
        
        # Load user feed file
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(resourceID % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Initialize return variables
        responseCode = 3
        
        # Execute collection
        for media in feed:
            echo.out(u"Media: %s." % media["id"])
            while (True):
                try:
                    header = {"User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
                    request = urllib2.Request(media["images"]["standard_resolution"]["url"], headers = header)
                    #request = urllib2.Request(media["images"]["standard_resolution"]["url"])
                    imageData = urllib2.urlopen(request).read()
                except Exception as error:
                    if type(error) == urllib2.HTTPError:
                        echo.out(error, "ERROR")
                        responseCode = -4
                        break
                    else:
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            echo.out(u"Request error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
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
        