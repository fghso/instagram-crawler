# -*- coding: iso-8859-1 -*-

import os
import json
import shutil
import stat
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
        self.connection = mysql.connector.connect(**self.config["connargs"][0]) 
        
        # Queries
        self.insertMedia = "INSERT INTO media (`users_pk_ref`, `id`, `type`, `filter`, `link`, `created_time`, `users_in_photo_count`, `tags_count`, `comments_count`, `likes_count`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        self.insertTags = "INSERT INTO tags (`media_pk_ref`, `tag`) VALUES (%s, %s)"
        self.insertImages = "INSERT INTO images (`media_pk_ref`, `low_res_url`, `thumbnail_url`, `std_res_url`) VALUES (%s, %s, %s, %s)"
        self.insertLocations = "INSERT INTO locations (`media_pk_ref`, `id`, `name`, `latitude`, `longitude`) VALUES (%s, %s, %s, %s, %s)"
        self.insertComments = "INSERT INTO comments (`media_pk_ref`, `id`, `created_time`, `text`, `from_id`, `from_profile_picture`) VALUES (%s, %s, %s, %s, %s, %s)"
        self.insertCaptions = "INSERT INTO captions (`media_pk_ref`, `id`, `created_time`, `text`, `from_id`, `from_profile_picture`) VALUES (%s, %s, %s, %s, %s, %s)"
        self.insertLikes = "INSERT INTO likes (`media_pk_ref`, `from_id`, `from_profile_picture`) VALUES (%s, %s, %s)"
                
    def crawl(self, resourceID, filters):
        self.echo.out(u"User ID received: %s." % resourceID)
        
        # Load user feed file
        feedsBaseDir = "../../data-update/feeds"
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
        

class FPPImporter(BaseCrawler):
    def __init__(self, configurationsDictionary):
        BaseCrawler.__init__(self, configurationsDictionary)
        self.instaConnection = mysql.connector.connect(**self.config["connargs"][0]) 
        self.crawlerConnection = mysql.connector.connect(**self.config["connargs"][1]) 
        
        # Queries
        self.selectMediaPK = "SELECT `media_pk` FROM `media` WHERE `id` = %s"
        self.selectUsers = "SELECT `users_pk` FROM `users` WHERE `id` = %s"
        self.selectComments = "SELECT `comments_pk` FROM `comments` WHERE `from_id` = %s LIMIT 1"
        self.selectLikes = "SELECT `likes_pk` FROM `likes` WHERE `from_id` = %s LIMIT 1"
        self.insertFaces = "INSERT INTO `faces` (`media_pk_ref`, `gender_value`, `gender_confidence`, `age_value`, `age_range`, `smiling_value`) VALUES (%s, %s, %s, %s, %s, %s)"
        self.insertCollectMedia = "INSERT INTO `collect_fpp_media` (`status`, `media_pk`, `id`, `faces_count`, `type`) VALUES (%s, %s, %s, %s, %s)"
        self.insertProfileFaces = "INSERT INTO `profile_faces` (`user_id`, `from_table`, `gender_value`, `gender_confidence`, `age_value`, `age_range`, `smiling_value`) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        self.insertCollectProfiles = "INSERT INTO `collect_fpp_profiles` (`status`, `id`, `from_table`, `faces_count`) VALUES (%s, %s, %s, %s)"
        self.insertExtraFaces = "INSERT INTO `extra_faces` (`media_id`, `gender_value`, `gender_confidence`, `age_value`, `age_range`, `smiling_value`) VALUES (%s, %s, %s, %s, %s, %s)"

    def crawl(self, resourceID, filters):
        # Configure data directories
        fppOldBaseDir = "../../data-cosn/fppmerge" 
        fppNewBaseDir = "../../data-cosn/reorgfpp" 
        fppDataDir = os.path.join(fppOldBaseDir, resourceID)

        # Open cursors
        instaCursor = self.instaConnection.cursor()
        crawlerCursor = self.crawlerConnection.cursor()
        instaCursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
        crawlerCursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")
        
        # Execute import
        try: 
            for fileName in os.listdir(fppDataDir):
                with open(os.path.join(fppDataDir, fileName), "r") as fileObject: fppContent = json.load(fileObject)
                fileBaseName = os.path.splitext(fileName)[0]
                entityID = fileBaseName.split("_")
            
                # Media object
                if len(entityID) > 1:
                    instaCursor.execute(self.selectMediaPK, (fileBaseName,))
                    result = instaCursor.fetchone()
                    data = []
                    if result:
                        mediaPK = result[0]
                        for face in fppContent["face"]:
                            data.append((mediaPK, face["attribute"]["gender"]["value"], face["attribute"]["gender"]["confidence"], face["attribute"]["age"]["value"], face["attribute"]["age"]["range"], face["attribute"]["smiling"]["value"]))
                        if data: instaCursor.executemany(self.insertFaces, data)
                        data = (2, mediaPK, fileBaseName, len(fppContent["face"]), "cache")
                        crawlerCursor.execute(self.insertCollectMedia, data)
                    else:
                        for face in fppContent["face"]:
                            data.append((fileBaseName, face["attribute"]["gender"]["value"], face["attribute"]["gender"]["confidence"], face["attribute"]["age"]["value"], face["attribute"]["age"]["range"], face["attribute"]["smiling"]["value"]))
                        if data: instaCursor.executemany(self.insertExtraFaces, data)
                        else: instaCursor.execute(self.insertExtraFaces, (fileBaseName, None, None, None, None, None))
                    
                    fppSubDir = os.path.join(fppNewBaseDir, "media", str(int(entityID[1]) % 1000))
                    if not os.path.exists(fppSubDir): os.makedirs(fppSubDir)
                    fppNewPath = os.path.join(fppSubDir, "%s.fpp" % fileBaseName)
                    shutil.copy(os.path.join(fppDataDir, fileName), fppNewPath)
                    os.chmod(fppNewPath, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
                    
                # Profile
                else:
                    fromTable = "users"
                    instaCursor.execute(self.selectUsers, (fileBaseName,))
                    result = instaCursor.fetchone()
                    if not result: 
                        fromTable = "comments"
                        instaCursor.execute(self.selectComments, (fileBaseName,))
                        result = instaCursor.fetchone()
                        if not result: 
                            fromTable = "likes"
                            instaCursor.execute(self.selectLikes, (fileBaseName,))
                            result = instaCursor.fetchone()
                            if not result: fromTable = "unknown"
                    data = []
                    for face in fppContent["face"]:
                        data.append((fileBaseName, fromTable, face["attribute"]["gender"]["value"], face["attribute"]["gender"]["confidence"], face["attribute"]["age"]["value"], face["attribute"]["age"]["range"], face["attribute"]["smiling"]["value"]))
                    if data: instaCursor.executemany(self.insertProfileFaces, data)
                    data = (2, fileBaseName, fromTable, len(fppContent["face"]))
                    crawlerCursor.execute(self.insertCollectProfiles, data)
                    
                    fppSubDir = os.path.join(fppNewBaseDir, "profiles", str(int(entityID[0]) % 1000))
                    if not os.path.exists(fppSubDir): os.makedirs(fppSubDir)
                    fppNewPath = os.path.join(fppSubDir, "%s.fpp" % fileBaseName)
                    shutil.copy(os.path.join(fppDataDir, fileName), fppNewPath)
                    os.chmod(fppNewPath, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH)
            
            self.instaConnection.commit()
            self.crawlerConnection.commit()
        except:
            self.instaConnection.rollback()
            self.crawlerConnection.rollback()
            raise   
            
        return (None, None, None)

      