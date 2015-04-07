# -*- coding: iso-8859-1 -*-

import os
import json
import common
from fpp.facepp import API
from fpp.facepp import APIError


class Crawler:
    def __init__(self, configurationsDictionary):
        self.config = configurationsDictionary
    
    def crawl(self, resourceID, filters):
        echo = common.EchoHandler(self.config)
        
        # Configura diretórios base para armazenamento
        fppBaseDir = "../../data/fppselfies"
        fppSubDir = str(int(resourceID.split("_")[0]) % 1000)
        fppDataDir = os.path.join(fppBaseDir, fppSubDir)
        if not os.path.exists(fppDataDir): os.makedirs(fppDataDir)
        
        # Inicializa variáveis de retorno
        extraInfo = {"mediaerrors": [], "output": []}
        
        # Verifica se o arquivo já existe
        fppFilePath = os.path.join(fppDataDir, "%s.json" % resourceID)
        if os.path.isfile(fppFilePath): 
            echo.out(u"Media %s alerady exists." % resourceID)
            return (None, extraInfo, None)
        
        # Extrai filtros
        imageURL = filters[0]["data"]["url"]
        application = filters[1]["data"]["application"]
    
        # Constrói objeto da API com as credenciais de acesso
        apiServer = application["apiserver"]
        apiKey = application["apikey"]
        apiSecret = application["apisecret"]
        api = API(srv = apiServer, key = apiKey, secret = apiSecret, timeout = 60, max_retries = 0, retry_delay = 0)
        echo.out(u"ID: %s (App: %s)." % (resourceID, application["name"]))
        
        # Executa coleta
        attributes = ["gender", "age", "race", "smiling", "glass", "pose"]
        try:
            response = api.detection.detect(url = imageURL, attribute = attributes)
        except Exception as error: 
            # Códigos de erro HTTP: http://www.faceplusplus.com/detection_detect/
            if isinstance(error, APIError): message = "%d: %s" % (error.code, json.loads(error.body)["error"])
            # socket.error e urllib2.URLError 
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
