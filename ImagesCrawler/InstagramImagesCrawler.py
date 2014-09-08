# -*- coding: iso-8859-1 -*-

from datetime import datetime, timedelta
import urllib2
import json
import time
import os
import logging


# O limite de requisições estipulado nos termos de uso do Instagram é de 5000 requisições por hora. 
# Por garantia o número máximo foi configurado para um valor um pouco abaixo desse limite 
maxRequestsPerHour = 4990

# Configura valores iniciais das variáveis de controle
requestsPerHour = 0

# Configura logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramImagesCrawler[%s].log" % app.name, filemode="w", level=logging.INFO)
requestLoggingInterval = 1000

# Configura tratamento de exceções
maxNumberOfRetrys = 10
retrys = 0
sleepSecondsMultiply = 0

# Define diretório onde estão armazenados os feeds
feedsDirectory = "CrawledData\Feeds"
        
# Executa coleta
logging.info(u"Iniciando requisições.")
firsRequestTime = datetime.now()
for dirName in os.listdir(feedsDirectory):
    userDirectory = os.path.join(feedsDirectory, dirName)
    
    # Cria diretório para armazenar imagens
    imagesDirectory = os.path.join("CrawledData\Images", dirName)
    if not os.path.exists(imagesDirectory):
        os.makedirs(imagesDirectory)

    for fileName in os.listdir(userDirectory):
            fileObj = open(os.path.join(userDirectory, fileName), "r")
            input = json.load(fileObj)
            fileObj.close()
            
            for mediaInfo in input:
                while(True):
                    # Se não for imagem, prossegue para a próxima mídia
                    if (mediaInfo["type"] != "image"):
                        break
                        
                    # Verifica se o número máximo de requisições por hora foi atingido e, caso tenha sido,
                    # pausa o programa por tempo suficiente para evitar infração dos termos de uso do Instagram
                    if (requestsPerHour > maxRequestsPerHour):
                        sleepSeconds = (timedelta(seconds=3610) - (datetime.now() - firsRequestTime)).total_seconds()
                        if (sleepSeconds > 0):
                            m, s = divmod(sleepSeconds, 60)
                            logging.info(u"Limite de requisições atingido. Dormindo agora por %02d:%02d minuto(s)..." % (m, s))   
                            time.sleep(sleepSeconds)
                        logging.info(u"Reiniciando contagem de requisições.")
                        requestsPerHour = 0
                        firsRequestTime = datetime.now()
                    elif (requestsPerHour != 0 ) and (requestsPerHour % requestLoggingInterval == 0):
                        logging.info(u"%d requisição(ões) realizada(s)." % requestsPerHour)
                    
                    requestsPerHour += 1
                    try:
                        header = {"User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
                        request = urllib2.Request(mediaInfo["images"]["standard_resolution"]["url"], headers = header)
                        #request = urllib2.Request(mediaInfo["images"]["standard_resolution"]["url"])
                        imageData = urllib2.urlopen(request).read()
                    except Exception as err:
                        if type(err) == urllib2.HTTPError:
                            if (err.code == 403 or err.code == 404):
                                print(err.code)
                                break
                        
                        # Caso o número de tentativas não tenha ultrapassado o máximo,
                        # experimenta aguardar um certo tempo antes da próxima tentativa 
                        if (retrys < maxNumberOfRetrys):
                            sleepSeconds = 2 ** sleepSecondsMultiply
                            logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Mídia: %s]" % (sleepSeconds, mediaInfo["id"]))
                            time.sleep(sleepSeconds)
                            sleepSecondsMultiply += 1
                            retrys += 1
                            continue
                        else:
                            logging.exception(u"Erro no cliente. Execução abortada.")
                            raise
                    else:
                        retrys = 0
                        sleepSecondsMultiply = 0
                        
                        imageFile = "[stdres]%s.jpg" % (mediaInfo["id"])
                        output = open(os.path.join(imagesDirectory, imageFile), "wb")
                        output.write(imageData)
                        output.close()
                        
                        break
                                