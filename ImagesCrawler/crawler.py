# -*- coding: iso-8859-1 -*-

import os
import socket
import time
import logging
import urllib2
import mysql.connector
import app


class Crawler:
    def __init__(self):
        # Abre conexão com o banco de dados
        self.mysqlConnection = mysql.connector.connect(user=app.dbUser, password=app.dbPassword, host=app.dbHost, database=app.dbName)
        self.cursor = self.mysqlConnection.cursor()
        
        # Define queries
        self.selectImageQuery = """SELECT medias_pk_ref, std_res_url FROM images WHERE images_pk = %s"""
        self.selectMediaQuery = """SELECT users_pk_ref, id, type FROM medias WHERE medias_pk = %s"""
        self.selectUserQuery = """SELECT id FROM users WHERE users_pk = %s"""

        
    # Retorna o nome que identifica o coletor
    def getName(self):
        return socket.gethostname()

        
    # Valores de retorno:
    #    2 => Coleta bem sucedida
    #   -2 => Não coletado pois a mídia é um vídeo
    #   -3 => urllib2.HTTPError - Erro 403
    #   -4 => urllib2.HTTPError - Erro 404
    def crawl(self, resourceID):
        # Configura logging
        logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                            filename="InstagramImagesCrawler[%s].log" % os.getpid(), filemode="w", level=logging.INFO)

        # Configura tratamento de exceções
        maxNumberOfRetrys = 10
        retrys = 0
        sleepSecondsMultiply = 0
        
        # Obtém informações da imagem
        self.cursor.execute(self.selectImageQuery, (resourceID,))
        imageInfo = self.cursor.fetchone()
        imageURL = imageInfo[1]
        
        # Obtém informações da mídia 
        self.cursor.execute(self.selectMediaQuery, (imageInfo[0],))
        mediaInfo = self.cursor.fetchone()
        mediaID = mediaInfo[1]
        mediaType = mediaInfo[2]
        
        # Obtém informações do usuário 
        self.cursor.execute(self.selectUserQuery, (mediaInfo[0],))
        userID = self.cursor.fetchone()[0]
        
        # Se for a mídia for um vídeo, retorna
        if (mediaType != "image"):
            return (-2, 1)
        
        # Cria diretório para armazenar a imagem
        imagesDirectory = os.path.join("../CrawledData/Images", userID)
        if not os.path.exists(imagesDirectory):
            os.makedirs(imagesDirectory)

        # Coleta imagem
        while(True):
            try:
                header = {"User-Agent" : "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11", "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
                request = urllib2.Request(imageURL, headers = header)
                #request = urllib2.Request(imageURL)
                imageData = urllib2.urlopen(request).read()
            except Exception as err:
                if type(err) == urllib2.HTTPError:
                    if (err.code == 403): 
                        return (-3, 1)
                    elif (err.code == 404): 
                        return (-4, 1)
                
                # Caso o número de tentativas não tenha ultrapassado o máximo,
                # experimenta aguardar um certo tempo antes da próxima tentativa 
                if (retrys < maxNumberOfRetrys):
                    sleepSeconds = 2 ** sleepSecondsMultiply
                    logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [Imagem: %s]" % (sleepSeconds, resourceID))
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
                
                imageFile = "[stdres]%s.jpg" % (mediaID)
                output = open(os.path.join(imagesDirectory, imageFile), "wb")
                output.write(imageData)
                output.close()
                
                return (2, 1)
        