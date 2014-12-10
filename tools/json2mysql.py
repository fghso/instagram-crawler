#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import os
import sys
import json
import logging
import glob
import datetime
from mysql.connector.errors import Error
from mysql.connector import errorcode
import mysql.connector
import config



# ==================== Configurações gerais ====================
# Debug
DEBUG = False

# Logging
logging.basicConfig(format="%(asctime)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                    filename="InstagramJSONToMySQL.log", filemode="w", level=logging.INFO)


# ==================== Banco de dados ====================
# Abre conexão com o banco de dados
mysqlConnection = mysql.connector.connect(user=config.dbUser, password=config.dbPassword, host=config.dbHost, database=config.dbName)
bufferedCursor = mysqlConnection.cursor(buffered=True)
cursor = mysqlConnection.cursor()

# Ajusta charset e collation da conexão
cursor.execute("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci")

# Define queries
selectUsersCountQuery = """SELECT count(*) FROM relationshipsToCollect;"""

selectUsersQuery = """SELECT resourceID FROM relationshipsToCollect A LEFT JOIN users B ON A.resourceID = B.id WHERE B.id IS NULL;"""

selectUserPKQuery = """SELECT users_pk FROM users WHERE id = %s;"""

insertUsersQuery = """INSERT INTO users (`id`, `username`, `full_name`, `profile_picture`, `bio`, `website`, `counts_media`, `counts_follows`, `counts_followed_by`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

insertUpdateUsersQuery = """INSERT INTO users (`id`, `username`, `full_name`, `profile_picture`, `bio`, `website`, `counts_media`, `counts_follows`, `counts_followed_by`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `counts_media` = VALUES(`counts_media`), `counts_follows` = VALUES(`counts_follows`), `counts_followed_by` = VALUES(`counts_followed_by`)"""

#updateUsersQuery = """UPDATE users SET `id` = %s, `username` = %s, `full_name` = %s, `profile_picture` = %s, `bio` = %s, `website` = %s, `counts_media` = %s, `counts_follows` = %s, `counts_followed_by` = %s WHERE `users_pk` = %s"""

insertRelationshipsQuery = """INSERT INTO relationships (`user_users_pk_ref`, `follower_users_pk_ref`) VALUES (%s, %s)"""

insertMediasQuery = """INSERT INTO medias (`users_pk_ref`, `id`, `type`, `filter`, `link`, `created_time`, `users_in_photo_count`, `tags_count`, `comments_count`, `likes_count`) VALUES (%s, %s, %s, %s, %s, FROM_UNIXTIME(%s), %s, %s, %s, %s)"""

insertImagesQuery = """INSERT INTO images (`medias_pk_ref`, `low_res_url`, `low_res_width`, `low_res_height`, `thumbnail_url`, `thumbnail_width`, `thumbnail_height`, `std_res_url`, `std_res_width`, `std_res_height`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        
insertTagsQuery = """INSERT INTO tags (`medias_pk_ref`, `tag`) VALUES (%s, %s)"""

insertLocationsQuery = """INSERT INTO locations (`medias_pk_ref`, `id`, `name`, `latitude`, `longitude`) VALUES (%s, %s, %s, %s, %s)"""

insertUsersInPhotoQuery = """INSERT INTO users_in_photo (`medias_pk_ref`, `user_id`, `position_x`, `position_y`) VALUES (%s, %s, %s, %s)"""

insertCommentsQuery = """INSERT INTO comments (`medias_pk_ref`, `id`, `created_time`, `text`, `from_user_id`) VALUES (%s, %s, FROM_UNIXTIME(%s), %s, %s)"""

insertCaptionsQuery = """INSERT INTO captions (`medias_pk_ref`, `id`, `created_time`, `text`, `from_user_id`) VALUES (%s, %s, FROM_UNIXTIME(%s), %s, %s)"""


# ==================== Funções de importação ====================
def importUser(userID):
    fileName = "../CrawledData/Users/%s.json" % userID
    fileObj = open(fileName, "r")
    userInfo = json.load(fileObj)
    fileObj.close()
    
    data = (userID, userInfo["username"], userInfo["full_name"], userInfo["profile_picture"], userInfo["bio"], userInfo["website"], userInfo["counts"]["media"], userInfo["counts"]["follows"], userInfo["counts"]["followed_by"])
    
    if not DEBUG: cursor.execute(insertUpdateUsersQuery, data)
    
    
def importRelationships(userID, usersPK):
    dirName = "../CrawledData/Relationships/%s/" % userID
        
    # Lista de seguidos
    for fileName in glob.iglob(dirName + "follows*"):
        fileObj = open(fileName, "r")
        input = json.load(fileObj)
        fileObj.close()
        
        for followsInfo in input:
            # Usuário seguido
            data = (followsInfo["id"], followsInfo["username"], followsInfo["full_name"], followsInfo["profile_picture"], followsInfo["bio"], followsInfo["website"], None, None, None)
            try:
                if not DEBUG: cursor.execute(insertUsersQuery, data)
                followsUsersPK = cursor.lastrowid
            except mysql.connector.Error as err:
                # Se o usuário já existir no banco de dados, obtém o valor da chave primária associada a ele
                if (err.errno == errorcode.ER_DUP_ENTRY):
                    cursor.execute(selectUserPKQuery, (followsInfo["id"],))
                    followsUsersPK = cursor.fetchone()[0]
                else:
                    logging.error(u"Erro na inserção do usuário seguido %s (path: %s)" % (followsInfo["id"], fileName))
                    raise
                
            # Relacionamento 
            data = (followsUsersPK, usersPK)
            try: 
                if not DEBUG: cursor.execute(insertRelationshipsQuery, data)
            except mysql.connector.Error as err:
                # Se o relacionamento já existir no banco de dados, ignora o erro
                if (err.errno == errorcode.ER_DUP_ENTRY): pass
                else:
                    logging.error(u"Erro na inserção do relacionamento com o usuário seguido %s (path: %s)" % (followsInfo["id"], fileName))
                    raise
        
    # Lista de seguidores
    for fileName in glob.iglob(dirName + "followedby*"):
        fileObj = open(fileName, "r")
        input = json.load(fileObj)
        fileObj.close()
        
        for followedByInfo in input:
            # Seguidor
            data = (followedByInfo["id"], followedByInfo["username"], followedByInfo["full_name"], followedByInfo["profile_picture"], followedByInfo["bio"], followedByInfo["website"], None, None, None)
            try: 
                if not DEBUG: cursor.execute(insertUsersQuery, data)
                followedbyUsersPK = cursor.lastrowid
            except mysql.connector.Error as err:
                # Se o usuário já existir no banco de dados, obtém o valor da chave primária associada a ele
                if (err.errno == errorcode.ER_DUP_ENTRY):
                    cursor.execute(selectUserPKQuery, (followedByInfo["id"],))
                    followedbyUsersPK = cursor.fetchone()[0]
                else:
                    logging.error(u"Erro na inserção do seguidor %s (path: %s)" % (followedByInfo["id"], fileName))
                    raise
                    
            # Relacionamento 
            data = (usersPK, followedbyUsersPK)
            try:
                if not DEBUG: cursor.execute(insertRelationshipsQuery, data)
            except mysql.connector.Error as err:
                # Se o relacionamento já existir no banco de dados, ignora o erro
                if (err.errno == errorcode.ER_DUP_ENTRY): pass
                else:
                    logging.error(u"Erro na inserção do relacionamento com o seguidor %s (path: %s)" % (followedByInfo["id"], fileName))
                    raise

                
def importMedia(userID, usersPK):
    dirName = "../CrawledData/Feeds/%s/" % userID
        
    for fileName in os.listdir(dirName):
        fileObj = open(dirName + fileName, "r")
        input = json.load(fileObj)
        fileObj.close()
        
        for mediaInfo in input:
            try:
                # Informações gerais
                data = (usersPK, mediaInfo["id"], mediaInfo["type"], mediaInfo["filter"], mediaInfo["link"], mediaInfo["created_time"], len(mediaInfo["users_in_photo"]), len(mediaInfo["tags"]), mediaInfo["comments"]["count"], mediaInfo["likes"]["count"])
                if not DEBUG: cursor.execute(insertMediasQuery, data)
                medias_pk = cursor.lastrowid
                
                # Imagens
                data = (medias_pk, mediaInfo["images"]["low_resolution"]["url"], mediaInfo["images"]["low_resolution"]["width"], mediaInfo["images"]["low_resolution"]["height"], mediaInfo["images"]["thumbnail"]["url"], mediaInfo["images"]["thumbnail"]["width"], mediaInfo["images"]["thumbnail"]["height"], mediaInfo["images"]["standard_resolution"]["url"], mediaInfo["images"]["standard_resolution"]["width"], mediaInfo["images"]["standard_resolution"]["height"])
                if not DEBUG: cursor.execute(insertImagesQuery, data)
                
                # Tags
                data = []
                for tag in mediaInfo["tags"]:
                    data.append((medias_pk, tag))
                if not DEBUG: cursor.executemany(insertTagsQuery, data)
                
                # Localização
                if mediaInfo["location"] is not None:
                    location = [medias_pk, None, None, None, None]
                    if "id" in mediaInfo["location"]: location[1] = mediaInfo["location"]["id"]
                    if "name" in mediaInfo["location"]: location[2] = mediaInfo["location"]["name"]
                    if "latitude" in mediaInfo["location"]: location[3] = mediaInfo["location"]["latitude"]
                    if "longitude" in mediaInfo["location"]: location[4] = mediaInfo["location"]["longitude"]
                    data = tuple(location)
                    if not DEBUG: cursor.execute(insertLocationsQuery, data)
                
                # Usuários na foto
                data = []
                for user in mediaInfo["users_in_photo"]:
                    data.append((medias_pk, user["user"]["id"], user["position"]["x"], user["position"]["y"]))
                if not DEBUG: cursor.executemany(insertUsersInPhotoQuery, data)
                
                # Comentários
                data = []
                if (mediaInfo["comments"]["count"] > 0):
                    commentsFile = "../CrawledData/Comments/%s/%s.json" % (userID, mediaInfo["id"])
                    try:
                        fileObj = open(commentsFile, "r")
                        commentsData = json.load(fileObj)
                        fileObj.close()
                    except IOError:
                        pass
                    else:
                        for comment in commentsData:
                            data.append((medias_pk, comment["id"], comment["created_time"], comment["text"], comment["from"]["id"]))
                        if not DEBUG: cursor.executemany(insertCommentsQuery, data)
                        
                # Captions
                if mediaInfo["caption"] is not None:
                    data = (medias_pk, mediaInfo["caption"]["id"], mediaInfo["caption"]["created_time"], mediaInfo["caption"]["text"], mediaInfo["caption"]["from"]["id"])
                    if not DEBUG: cursor.execute(insertCaptionsQuery, data)
                
            except mysql.connector.Error as err:
                logging.error(u"Erro na inserção de dados da mídia %s (path: %s)" % (mediaInfo["id"], dirName + fileName))
                raise


# ==================== Programa principal ====================
# Obtém o número total de usuários a serem importados e a lista dos que ainda faltam para importar
cursor.execute(selectUsersCountQuery)
total = cursor.fetchone()[0]
bufferedCursor.execute(selectUsersQuery)

# Inicializa variáveis de progresso
processed = float(total - bufferedCursor.rowcount)
errors = 0
msgPrevious = ""
msgNext = ""
startTime = datetime.datetime.today()

print "Importando dados..."
for userID in bufferedCursor:
    failed = errors

    # Insere informações do usuário no banco
    try:
        importUser(userID[0])
        usersPK = cursor.lastrowid
    except:
        errors += 1
        logging.exception(u"Erro na importação de informações do usuário %s" % userID[0])        
    else:
        # Insere informações dos relacionamentos no banco
        try:
            importRelationships(userID[0], usersPK)
        except:
            errors += 1
            logging.exception(u"Erro na importação de relacionamentos para o usuário %s" % userID[0])
        else:
            # Insere informações das mídias no banco
            try:
                importMedia(userID[0], usersPK)
            except:
                errors += 1
                logging.exception(u"Erro na importação de mídias para o usuário %s" % userID[0])
        
    # Salva informações no banco de dados ou cancela a transação em caso de erros
    if not DEBUG: 
        if (failed == errors): 
            mysqlConnection.commit()
        else:
            mysqlConnection.rollback()
        
    # Imprime informações de progresso na tela
    processed += 1
    processedPercent = (processed / total) * 100
    successPercent = ((processed - errors) / total) * 100
    timeElapsed = datetime.datetime.today() - startTime
    elapsedHours, elapsedRemainder = divmod(timeElapsed.total_seconds(), 3600)
    elapsedMinutes, elapsedSeconds = divmod(elapsedRemainder, 60) 
    timeElapsedFormated = "%dh%02dmin" % (elapsedHours, elapsedMinutes)
    msgPrevious = msgNext
    msgNext = "  Processado: {0:5.1f}% ({1:.1f}% ok / {2:d} erros) em {3:%d-%m-%Y %H:%M:%S} ({4:s} em execucao)".format(processedPercent, successPercent, errors, datetime.datetime.today(), timeElapsedFormated)
    sys.stdout.write("\r" + msgNext + (" " * (len(msgPrevious) - len(msgNext))))
    sys.stdout.flush()
    
print "\nConcluido."  
