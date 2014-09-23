#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import json
import time
import os
import logging
from datetime import datetime, timedelta
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
import app


# Constrói objeto da API com as credenciais de acesso
api = InstagramAPI(client_id=app.clientID, client_secret=app.clientSecret)

# O limite de requisições estipulado nos termos de uso do Instagram é de 5000 requisições por hora. 
# Por garantia o número máximo foi configurado para um valor um pouco abaixo desse limite 
maxRequestsPerHour = 4990

# Configura logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%m/%d/%Y %H:%M:%S", 
                    filename="InstagramTagRecentMediaCrawler[%s].log" % app.appName, filemode="w", level=logging.INFO)
requestLoggingInterval = 1000

# Configura tratamento de exceções
maxNumberOfRetrys = 10
retrys = 0
sleepSecondsMultiply = 0

# Cria diretório para armazenar todas as mídias
recentMediaRoot = "CrawledData"
if not os.path.exists(recentMediaRoot):
    os.mkdir(recentMediaRoot)

# Executa coleta
logging.info("Iniciando requisições.")
nextSelfieRecentMediaPage = ""
maxID = ""
fileNumber = 1
requestsPerHour = 0
firsRequestTime = datetime.now()
while(True):
    # Verifica se o número máximo de requisições por hora foi atingido e, caso tenha sido,
    # pausa o programa por tempo suficiente para evitar infração dos termos de uso do Instagram
    if (requestsPerHour > maxRequestsPerHour):
        sleepSeconds = (timedelta(seconds=3610) - (datetime.now() - firsRequestTime)).total_seconds()
        if (sleepSeconds > 0):
            m, s = divmod(sleepSeconds, 60)
            logging.info("Limite de requisições atingido. Dormindo agora por %02d:%02d minuto(s)..." % (m, s))   
            time.sleep(sleepSeconds)
        logging.info("Reiniciando contagem de requisições.")
        requestsPerHour = 0
        firsRequestTime = datetime.now()
    elif (requestsPerHour != 0 ) and (requestsPerHour % requestLoggingInterval == 0):
        logging.info("%d requisição(ões) realizada(s)." % requestsPerHour)
        
    requestsPerHour += 1
    
    # Parseia url da próxima página para extrair o max_id
    urlParts = nextSelfieRecentMediaPage.split("&")
    if len(urlParts) > 2:
        maxID = urlParts[2].split("=")[1]
    
    try:
        # Executa requisição na API para obter dados do usuário
        selfieRecentMedia, nextSelfieRecentMediaPage = api.tag_recent_media(count=30, tag_name="selfie", return_json=True, max_tag_id=maxID)
        #selfieRecentMedia, nextSelfieRecentMediaPage = api.tag_recent_media(count=30, tag_name="selfie", return_json=True)
    except:
        # Caso o número de tentativas não tenha ultrapassado o máximo,
        # Experimenta aguardar um certo tempo antes da próxima tentativa
        if (retrys < maxNumberOfRetrys):
            sleepSeconds = 2 ** sleepSecondsMultiply
            logging.warning("Erro na chamada à API. Tentando novamente em %02d segundo(s)." % sleepSeconds)
            time.sleep(sleepSeconds)
            sleepSecondsMultiply += 1
            retrys += 1
            continue
        else:
            logging.exception("Erro na chamada à API. Execução abortada.")
            raise
    else:      
        retrys = 0
        sleepSecondsMultiply = 0
        
        # Cria diretório para armazenar mídias do dia, caso este não exista ainda
        folderName = datetime.today().strftime("%Y%m%d")
        dir = os.path.join(recentMediaRoot, folderName)
        if not os.path.exists(dir):
            os.mkdir(dir)
            fileNumber = 1
    
        # Salva arquivo JSON com informações sobre as mídias coletadas
        fileName = "%s%05d.json" % (folderName, fileNumber)
        output = open(os.path.join(dir, fileName), "w")
        json.dump(selfieRecentMedia, output)
        output.close()
        fileNumber += 1
        print("%s: %s" % (fileName, maxID))       
        