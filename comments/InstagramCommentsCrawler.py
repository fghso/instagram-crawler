#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

from instagram.client import InstagramAPI
from instagram.bind import InstagramAPIError
from datetime import datetime, timedelta
import mysql.connector
import json
import time
import os
import logging
import app


# Constr�i objeto da API com as credenciais de acesso
api = InstagramAPI(client_id=app.clientID, client_secret=app.clientSecret)

# O limite de requisi��es estipulado nos termos de uso do Instagram � de 5000 requisi��es por hora. 
# Por garantia o n�mero m�ximo foi configurado para um valor um pouco abaixo desse limite 
maxRequestsPerHour = 4990

# Configura valores iniciais das vari�veis de controle
requestsPerHour = 0

# Configura logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramCommentsCrawler[%s].log" % app.name, filemode="w", level=logging.INFO)
requestLoggingInterval = 1000

# Configura tratamento de exce��es
maxNumberOfRetrys = 10
retrys = 0
sleepSecondsMultiply = 0

# Abre conex�o com o banco de dados
mysqlConnection = mysql.connector.connect(user=app.dbUser, password=app.dbPassword, host=app.dbHost, database=app.dbName)
bufferedCursor = mysqlConnection.cursor(buffered=True)
cursor = mysqlConnection.cursor()

# Define queries
selectDaysQuery = """SELECT DATE(updated_at) AS updated_at_days FROM usersToCollect WHERE updated_at IS NOT NULL GROUP BY DATE(updated_at)"""

#selectUsersQuery = """SELECT pinterestID FROM usersToCollect WHERE statusColeta = 2 AND updated_at BETWEEN %s AND %s ORDER BY updated_at ASC LIMIT %s;"""

selectUsersCollectedCountQuery = """SELECT count(*) FROM usersToCollect A INNER JOIN commentsToCollect B ON A.pinterestID = B.pinterestID WHERE (A.statusColeta = 2) AND (A.updated_at BETWEEN %s AND %s);"""

selectUsersQuery = """SELECT A.pinterestID FROM usersToCollect A LEFT JOIN commentsToCollect B ON A.pinterestID = B.pinterestID WHERE (A.statusColeta = 2) AND (A.updated_at BETWEEN %s AND %s) AND (B.pinterestID IS NULL) ORDER BY A.updated_at ASC LIMIT %s;"""

insertCommentsQuery = """INSERT INTO commentsToCollect (`pinterestID`, `statusColeta`, `crawler`, `qtd`) VALUES (%s, %s, %s, %s)"""

updateCommentsQuery = """UPDATE commentsToCollect SET statusColeta = %s, qtd = %s WHERE pinterestID = %s""";

# Define o n�mero de IDs por dia a considerar
idsPerDay = 1000

# Obt�m lista de dias de coleta
cursor.execute(selectDaysQuery)
days = cursor.fetchall()

# Executa coleta
logging.info(u"Iniciando requisi��es.")
firsRequestTime = datetime.now()
for day in days[:6]:
#for day in days[6:12]:
#for day in days[12:]:
    # Define quantos IDs ainda precisam ser coletados para o dia especificado a fim de completar a cota de IDs por dia
    cursor.execute(selectUsersCollectedCountQuery, (day[0], day[0] + timedelta(days=1)))
    idsCollectedCount = cursor.fetchone()
    idsToCollect = idsPerDay - idsCollectedCount[0] if idsCollectedCount[0] <= idsPerDay else 0

    # Obt�m IDs para um determinado dia
    bufferedCursor.execute(selectUsersQuery, (day[0], day[0] + timedelta(days=1), idsToCollect))
    
    for userID in bufferedCursor:
        # Marca in�cio da coleta do usu�rio no banco de dados
        cursor.execute(insertCommentsQuery, (userID[0], 0, app.name, 0))
        mysqlConnection.commit()
        
        dirFeed = "../CrawledData/Feeds/%s/" % userID[0]
        statusColeta = 2
        qtd = 0
        
        for fileName in os.listdir(dirFeed):
            fileObj = open(dirFeed + fileName, "r")
            input = json.load(fileObj)
            fileObj.close()
            
            # Cria diret�rio para armazenar coment�rios
            dirComments = "../CrawledData/Comments/%s" % userID[0]
            if not os.path.exists(dirComments):
                os.makedirs(dirComments)
            
            for mediaInfo in input:
                if (mediaInfo["comments"]["count"] > 0):
                    while(True):
                        # Verifica se o n�mero m�ximo de requisi��es por hora foi atingido e, caso tenha sido,
                        # pausa o programa por tempo suficiente para evitar infra��o dos termos de uso do Instagram
                        if (requestsPerHour > maxRequestsPerHour):
                            sleepSeconds = (timedelta(seconds=3610) - (datetime.now() - firsRequestTime)).total_seconds()
                            if (sleepSeconds > 0):
                                m, s = divmod(sleepSeconds, 60)
                                logging.info(u"Limite de requisi��es atingido. Dormindo agora por %02d:%02d minuto(s)..." % (m, s))   
                                time.sleep(sleepSeconds)
                            logging.info(u"Reiniciando contagem de requisi��es.")
                            requestsPerHour = 0
                            firsRequestTime = datetime.now()
                        elif (requestsPerHour != 0 ) and (requestsPerHour % requestLoggingInterval == 0):
                            logging.info(u"%d requisi��o(�es) realizada(s)." % requestsPerHour)
                    
                        requestsPerHour += 1
                        
                        try:
                            # Executa requisi��o na API para obter os coment�rios da m�dia
                            mediaComments = api.media_comments(media_id=mediaInfo["id"], return_json=True)
                        except InstagramAPIError as err:
                            if err.error_type == "APINotFoundError":
                                statusColeta = 1
                                break
                            else:
                                # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                                # experimenta aguardar um certo tempo antes da pr�xima tentativa
                                if (retrys < maxNumberOfRetrys):
                                    sleepSeconds = 2 ** sleepSecondsMultiply
                                    logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [M�dia: %s - Path: %s]" % (sleepSeconds, mediaInfo["id"], dirFeed + fileName))
                                    time.sleep(sleepSeconds)
                                    sleepSecondsMultiply += 1
                                    retrys += 1
                                    continue
                                else:
                                    logging.exception(u"Erro no cliente. Execu��o abortada. [M�dia: %s - Path: %s]" % (mediaInfo["id"], dirFeed + fileName))
                                    raise
                        except:
                            # Caso o n�mero de tentativas n�o tenha ultrapassado o m�ximo,
                            # experimenta aguardar um certo tempo antes da pr�xima tentativa
                            if (retrys < maxNumberOfRetrys):
                                sleepSeconds = 2 ** sleepSecondsMultiply
                                logging.warning(u"Erro no cliente. Tentando novamente em %02d segundo(s). [M�dia: %s - Path: %s]" % (sleepSeconds, mediaInfo["id"], dirFeed + fileName))
                                time.sleep(sleepSeconds)
                                sleepSecondsMultiply += 1
                                retrys += 1
                                continue
                            else:
                                logging.exception(u"Erro no cliente. Execu��o abortada. [M�dia: %s - Path: %s]" % (mediaInfo["id"], dirFeed + fileName))
                                raise
                        else:
                            qtd += 1
                            retrys = 0
                            sleepSecondsMultiply = 0
                        
                            filename = "%s.json" % mediaInfo["id"]
                            output = open(os.path.join(dirComments, filename), "w")
                            json.dump(mediaComments, output)
                            output.close()
                            
                            break
                                
        # Marca final da coleta do usu�rio no banco de dados
        cursor.execute(updateCommentsQuery, (statusColeta, qtd, userID[0]))
        mysqlConnection.commit()
