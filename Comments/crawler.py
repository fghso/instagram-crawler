# -*- coding: iso-8859-1 -*-

import os
import json
import time
import logging
from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
import app


class Crawler:
    # Retorna o nome que identifica o coletor
    def getName:
        return app.name
        
    # Valores de retorno:
    #   2 => Coleta bem sucedida
    #   3 => Coleta parcialmente bem sucedida
    def crawl(self, resourceID):
        status = 2
        amount = 0
    
        # Constrói objeto da API com as credenciais de acesso
        api = InstagramAPI(client_id = app.clientID, client_secret = app.clientSecret)
    
        # Configura logging
        logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                            filename="InstagramCommentsCrawler[%s].log" % app.name, filemode="w", level=logging.INFO)
    
        # Configura tratamento de exceções
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 0
    
        # Executa coleta dos comentários
        dirFeed = "../CrawledData/Feeds/%s/" % resourceID
        for fileName in os.listdir(dirFeed):
            fileObj = open(dirFeed + fileName, "r")
            input = json.load(fileObj)
            fileObj.close()
            
            # Cria diretório para armazenar comentários
            dirComments = "../CrawledData/Comments/%s" % resourceID
            if not os.path.exists(dirComments):
                os.makedirs(dirComments)
            
            for mediaInfo in input:
                if (mediaInfo["comments"]["count"] > 0):
                    while(True):
                        try:
                            # Executa requisição na API para obter os comentários da mídia
                            mediaComments = api.media_comments(media_id=mediaInfo["id"], return_json=True)
                        except InstagramAPIError as err:
                            if err.error_type == "APINotFoundError":
                                status = 3
                                break
                            else:
                                # Caso o número de tentativas não tenha ultrapassado o máximo,
                                # experimenta aguardar um certo tempo antes da próxima tentativa
                                if (retrys < maxNumberOfRetrys):
                                    sleepSeconds = 2 ** sleepSecondsMultiply
                                    logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Mídia: %s - Path: %s]" % (sleepSeconds, mediaInfo["id"], dirFeed + fileName))
                                    time.sleep(sleepSeconds)
                                    sleepSecondsMultiply += 1
                                    retrys += 1
                                    continue
                                else:
                                    logging.exception(u"Erro no cliente. Execução abortada. [Mídia: %s - Path: %s]" % (mediaInfo["id"], dirFeed + fileName))
                                    raise
                        except:
                            # Caso o número de tentativas não tenha ultrapassado o máximo,
                            # experimenta aguardar um certo tempo antes da próxima tentativa
                            if (retrys < maxNumberOfRetrys):
                                sleepSeconds = 2 ** sleepSecondsMultiply
                                logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Mídia: %s - Path: %s]" % (sleepSeconds, mediaInfo["id"], dirFeed + fileName))
                                time.sleep(sleepSeconds)
                                sleepSecondsMultiply += 1
                                retrys += 1
                                continue
                            else:
                                logging.exception(u"Erro no cliente. Execução abortada. [Mídia: %s - Path: %s]" % (mediaInfo["id"], dirFeed + fileName))
                                raise
                        else:
                            retrys = 0
                            sleepSecondsMultiply = 0
                        
                            # Salva arquivo JSON com os comentários
                            filename = "%s.json" % mediaInfo["id"]
                            output = open(os.path.join(dirComments, filename), "w")
                            json.dump(mediaComments, output)
                            output.close()
                            
                            amount += 1
                            break
        
        return (status, amount)
