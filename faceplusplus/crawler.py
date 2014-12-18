# -*- coding: iso-8859-1 -*-

import os
import socket
import logging
import json
from fpp.facepp import API
import fpp.apikey


class Crawler:
    def crawl(self, resourceID, filters):
        dataDir = "data"
        if not os.path.exists(dataDir): os.makedirs(dataDir)
        
        try: 
            attrs = ['gender', 'age', 'race', 'smiling', 'glass', 'pose']
            api = API(key=fpp.apikey.API_KEY, secret=fpp.apikey.API_SECRET, srv=fpp.apikey.SERVER)
            response = api.detection.detect(url = filters[0]["data"]["url"], attribute = attrs)
            json.dump(response, open(os.path.join(dataDir, "%s.json" % resourceID), "w"), indent=4)
        except Exception as error:
            logging.exception("Exception doing request for media %s." % resourceID)
            raise
        
        return ({"crawler_name": socket.gethostname()}, None)

    def clean(self):
        pass
        