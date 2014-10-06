#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import json
import xml.etree.ElementTree as ET
import crawler


try:
    CONFIGFILE = sys.argv[1]
except:
    sys.exit("usage: %s configFilePath" % sys.argv[0])

# Carrega configurações
config = ET.parse(CONFIGFILE)
cfgAddress = config.findtext("./connection/address")
cfgPort = int(config.findtext("./connection/port"))
cfgBufsize = int(config.findtext("./connection/bufsize"))

# Cria uma instância do coletor
crawlerObj = crawler.Crawler()

# Recebe ID e parâmetros do servidor
processID = os.getpid()
server = socket.socket()
server.connect((cfgAddress, cfgPort))
server.send(json.dumps({"command": "GET_LOGIN", "name": crawlerObj.getName(), "processid": processID}))
response = server.recv(cfgBufsize)
message = json.loads(response)
clientID = message["clientid"]
clientParams = message["clientparams"]
print "Conectado ao servidor com o ID: %s " % clientID

# Envia comando para receber um ID de recurso
server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))

while (True):
    try:
        response = server.recv(cfgBufsize)

        # Extrai comando recebido do servidor
        message = json.loads(response)
        command = message["command"]
        
        if (command == "GIVE_ID"):
            # Repassa o ID para o coletor
            resourceID = message["resourceid"]
            crawlerResponse = crawlerObj.crawl(resourceID, **clientParams)
            
            # Comunica ao servidor que a coleta do recurso foi finalizada
            server.send(json.dumps({"command": "DONE_ID", "clientid": clientID, "resourceid": resourceID, "status": crawlerResponse[0], "amount": crawlerResponse[1]}))

        elif (command == "DID_OK"):
            # Envia comando para receber um ID de recurso
            server.send(json.dumps({"command": "GET_ID", "clientid": clientID}))
            
        elif (command == "FINISH"):
            print "Tarefa concluida, cliente finalizado."
            break
            
        elif (command == "KILL"):
            print "Cliente removido pelo servidor."
            break
            
    except Exception as error:
        print "ERRO: %s" % str(error)
        excType, excObj, excTb = sys.exc_info()
        fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
        print (excType, fileName, excTb.tb_lineno)
        break

server.shutdown(socket.SHUT_RDWR)
server.close()
