# -*- coding: iso-8859-1 -*-

import os
import json
import common
import mysql.connector


class BaseCrawler:
    def __init__(self, configurationsDictionary):
        self._extractConfig(configurationsDictionary)
        self.echo = common.EchoHandler(self.config["echo"])
       
    def _extractConfig(self, configurationsDictionary):
        self.config = configurationsDictionary
        if ("echo" not in self.config): self.config["echo"] = {}
    
    def crawl(self, resourceID, filters):
        return (None, None, None)

        
class FeedsImporter(BaseCrawler):  
    def __init__(self, configurationsDictionary):
        BaseCrawler.__init__(self, configurationsDictionary)
        self.connection = mysql.connector.connect(**self.config["connargs"]) 
        
        # INSERT queries
        self.insertMedia = "INSERT INTO media (`users_pk_ref`, `id`, `type`, `filter`, `link`, `created_time`, `users_in_photo_count`, `tags_count`, `comments_count`, `likes_count`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.insertTags = "INSERT INTO tags (`media_pk_ref`, `tag`) VALUES (%s, %s)"
        self.insertImages = "INSERT INTO images (`media_pk_ref`, `low_res_url`, `thumbnail_url`, `std_res_url`) VALUES (%s, %s, %s, %s)"
        self.insertLocations = "INSERT INTO locations (`media_pk_ref`, `id`, `name`, `latitude`, `longitude`) VALUES (%s, %s, %s, %s, %s)"
        self.insertComments = "INSERT INTO comments (`media_pk_ref`, `id`, `created_time`, `text`, `from_id`, `from_profile_picture`) VALUES (%s, %s, %s, %s, %s, %s)"
        self.insertCaptions = "INSERT INTO captions (`media_pk_ref`, `id`, `created_time`, `text`, `from_id`, `from_profile_picture`) VALUES (%s, %s, %s, %s, %s, %s)"
        self.insertLikes = "INSERT INTO likes (`media_pk_ref`, `from_id`, `from_profile_picture`) VALUES (%s, %s, %s)"
        
        # ON DUPLICATE KEY UPDATE clauses
        if self.config["onduplicateupdate"]:
            self.insertMedia += " ON DUPLICATE KEY UPDATE `media_pk` = LAST_INSERT_ID(`media_pk`), `users_pk_ref` = VALUES(`users_pk_ref`), `id` = VALUES(`id`), `type` = VALUES(`type`), `filter` = VALUES(`filter`), `link` = VALUES(`link`), `created_time` = VALUES(`created_time`), `users_in_photo_count` = VALUES(`users_in_photo_count`), `tags_count` = VALUES(`tags_count`), `comments_count` = VALUES(`comments_count`), `likes_count` = VALUES(`likes_count`)"
            self.insertTags += " ON DUPLICATE KEY UPDATE `media_pk_ref` = VALUES(`media_pk_ref`), `tag` = VALUES(`tag`)"
            self.insertImages += " ON DUPLICATE KEY UPDATE `media_pk_ref` = VALUES(`media_pk_ref`), `low_res_url` = VALUES(`low_res_url`), `thumbnail_url` = VALUES(`thumbnail_url`), `std_res_url` = VALUES(`std_res_url`)"
            self.insertLocations += " ON DUPLICATE KEY UPDATE `media_pk_ref` = VALUES(`media_pk_ref`), `id` = VALUES(`id`), `name` = VALUES(`name`), `latitude` = VALUES(`latitude`), `longitude` = VALUES(`longitude`)"
            self.insertComments += " ON DUPLICATE KEY UPDATE `media_pk_ref` = VALUES(`media_pk_ref`), `id` = VALUES(`id`), `created_time` = VALUES(`created_time`), `text` = VALUES(`text`), `from_id` = VALUES(`from_id`), `from_profile_picture` = VALUES(`from_profile_picture`)"
            self.insertCaptions += " ON DUPLICATE KEY UPDATE `media_pk_ref` = VALUES(`media_pk_ref`), `id` = VALUES(`id`), `created_time` = VALUES(`created_time`), `text` = VALUES(`text`), `from_id` = VALUES(`from_id`), `from_profile_picture` = VALUES(`from_profile_picture`)"
            self.insertLikes += " ON DUPLICATE KEY UPDATE `media_pk_ref` = VALUES(`media_pk_ref`), `from_id` = VALUES(`from_id`), `from_profile_picture` = VALUES(`from_profile_picture`)"
        
    def _extractConfig(self, configurationsDictionary):
        BaseCrawler._extractConfig(self, configurationsDictionary)
        if ("onduplicateupdate" not in self.config): self.config["onduplicateupdate"] = False
        else: self.config["onduplicateupdate"] = common.str2bool(self.config["onduplicateupdate"])

    def crawl(self, resourceID, filters):
        self.echo.out(u"User ID received: %s." % resourceID)
        
        # Load user feed file
        feedsBaseDir = "../../data/feeds"
        feedsFilePath = os.path.join(feedsBaseDir, str(int(resourceID) % 1000), "%s.feed" % resourceID)
        with open(feedsFilePath, "r") as feedFile: feed = json.load(feedFile)
        
        # Extract filters
        userPK = filters[0]["data"]["users_pk"]
        
        # Execute import
        cursor = self.connection.cursor()
        cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
        try:
            for mediaObj in feed:
                # General information
                data = (userPK, mediaObj["id"], mediaObj["type"], mediaObj["filter"], mediaObj["link"], mediaObj["created_time"], len(mediaObj["users_in_photo"]), len(mediaObj["tags"]), mediaObj["comments"]["count"], mediaObj["likes"]["count"])
                cursor.execute(self.insertMedia, data)
                mediaPK = cursor.lastrowid
                
                # Tags
                data = []
                for tag in mediaObj["tags"]:
                    data.append((mediaPK, tag))
                cursor.executemany(self.insertTags, data)
                
                # Images
                data = (mediaPK, mediaObj["images"]["low_resolution"]["url"], mediaObj["images"]["thumbnail"]["url"], mediaObj["images"]["standard_resolution"]["url"])
                cursor.execute(self.insertImages, data)
                
                # Locations
                if mediaObj["location"] is not None:
                    data = [mediaPK, None, None, None, None]
                    if "id" in mediaObj["location"]: data[1] = mediaObj["location"]["id"]
                    if "name" in mediaObj["location"]: data[2] = mediaObj["location"]["name"]
                    if "latitude" in mediaObj["location"]: data[3] = mediaObj["location"]["latitude"]
                    if "longitude" in mediaObj["location"]: data[4] = mediaObj["location"]["longitude"]
                    cursor.execute(self.insertLocations, data)
                
                # Comments
                data = []
                if (mediaObj["comments"]["count"] > 0):
                    for comment in mediaObj["comments"]["data"]:
                        data.append((mediaPK, comment["id"], comment["created_time"], comment["text"], comment["from"]["id"], comment["from"]["profile_picture"]))
                    cursor.executemany(self.insertComments, data)
                        
                # Captions
                if mediaObj["caption"] is not None:
                    data = (mediaPK, mediaObj["caption"]["id"], mediaObj["caption"]["created_time"], mediaObj["caption"]["text"], mediaObj["caption"]["from"]["id"], mediaObj["caption"]["from"]["profile_picture"])
                    cursor.execute(self.insertCaptions, data)
                    
                # Likes
                data = []
                for like in mediaObj["likes"]["data"]:
                    data.append((mediaPK, like["id"], like["profile_picture"]))
                cursor.executemany(self.insertLikes, data)
            self.connection.commit()
        except:
            self.connection.rollback()
            raise
        
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

        

      