# -*- coding: iso-8859-1 -*-

"""Module to store crawler classes.

More than one class can be written here, but only one (that specified in the configuration file) will be used by the client to instantiate a crawler object whose :meth:`crawl() <BaseCrawler.crawl>` method will by called to do the collection of the resource received. 

"""

import os
import socket
import json
import time
import common
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from instagram.bind import InstagramClientError


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

        
class UsersCrawler(BaseCrawler):
    # Response codes:
    #    3 => Successful collection
    #   -4 => APINotAllowedError - you cannot view this resource
    #   -5 => APINotFoundError - this user does not exist
    def crawl(self, resourceID, filters):
        echo = common.EchoHandler(self.config)
        echo.out(u"User ID received: %s." % resourceID)
        
        # Extract filters
        application = filters[0]["data"]["application"]
            
        # Get authenticated API object
        clientID = application["clientid"]
        clientSecret = application["clientsecret"]
        api = InstagramAPI(client_id = clientID, client_secret = clientSecret)
        echo.out(u"App: %s." % str(application["name"]))

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
                        echo.out(u"API call error. Trying again in %02d second(s)." % sleepSeconds, "EXCEPTION")
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
        