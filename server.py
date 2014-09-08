#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import socket
import select
import json
import xml.etree.ElementTree as ET
import datetime
import mysql.connector


try:
    CONFIGFILE = sys.argv[1]
except:
    sys.exit("usage: %s configFilePath" % sys.argv[0])
    
# Carrega configurações
config = ET.parse(CONFIGFILE)
cfgAddress = config.findtext("./connection/address")
cfgPort = int(config.findtext("./connection/port"))
cfgBufsize = int(config.findtext("./connection/bufsize"))
cfgDBUser = config.findtext("./database/user")
cfgDBPassword = config.findtext("./database/password")
cfgDBHost = config.findtext("./database/host")
cfgDBName = config.findtext("./database/name")
cfgDBTable = config.findtext("./database/table")

# Abre conexão com o banco de dados
mysqlConnection = mysql.connector.connect(user=cfgDBUser, password=cfgDBPassword, host=cfgDBHost, database=cfgDBName)

# Define funções de interação com o banco de dados
def selectResource():
    query = "SELECT resourceID FROM " + cfgDBTable + " WHERE status IS NULL OR status = 0 ORDER BY rand() LIMIT 1"
    cursor = mysqlConnection.cursor()
    cursor.execute(query)
    resource = cursor.fetchone()
    return resource[0] if (resource != None) else resource
    
def updateResource(resourceID, status, amount, crawler):
    query = "UPDATE " + cfgDBTable + " SET status = %s, amount = %s, crawler = %s WHERE resourceID = %s"
    cursor = mysqlConnection.cursor()
    cursor.execute(query, (status, amount, crawler, resourceID))
    mysqlConnection.commit()
    
def collectedResourcesPercent():
    query = "SELECT count(resourceID) FROM " + cfgDBTable
    cursor = mysqlConnection.cursor()
    cursor.execute(query)
    resourcesTotal = float(cursor.fetchone()[0])
    query = "SELECT count(resourceID) FROM " + cfgDBTable + " WHERE status IS NOT NULL AND status != 0 AND status != 1"
    cursor = mysqlConnection.cursor()
    cursor.execute(query)
    resourcesCollected = float(cursor.fetchone()[0])
    return (resourcesCollected / resourcesTotal) * 100
    
# Define funções auxiliares
def removeClient(clientID):
    clientName = connectedClients[clientID][0]
    clientResourceID = connectedClients[clientID][3]
    updateResource(clientResourceID, 0, 0, clientName)
    del connectedClients[clientID]
    
def closeConnection(clientSocket):
    clientSocket.shutdown(socket.SHUT_RDWR)
    clientSocket.close()
    connectedSockets.remove(clientSocket)

# Inicializa servidor
server = socket.socket()
server.bind((cfgAddress, cfgPort))
server.listen(socket.SOMAXCONN)
connectedClients = {}
connectedSockets = [server]
nextFreeID = 1
shutdown = False
running = True

print "Servidor pronto. Aguardando conexoes..."
while (running):
    # Aguarda o recebimento de novos dados
    inputReady, outputReady, exceptionReady = select.select(connectedSockets, [], [])

    for client in inputReady:
        # Processa novas conexões
        if (client == server): 
            clientSocket, clientAddress = server.accept()
            connectedSockets.append(clientSocket)
            
        # Processa comandos de clientes já conectados
        else:
            response = client.recv(cfgBufsize)

            if (response):
                try:
                    message = json.loads(response)
                    command = message["command"]
                    
                    if (command == "GET_LOGIN"):
                        clientID = nextFreeID
                        clientName = message["name"]
                        #clientAddress = (socket.gethostbyaddr(client.getpeername()[0])[0], client.getpeername()[1])
                        clientAddress = client.getpeername()
                        clientPid = message["processid"]
                        connectedClients[clientID] = [clientName, clientAddress, clientPid, None, 0, None]
                        nextFreeID += 1
                        client.send(json.dumps({"command": "GIVE_LOGIN", "clientid": clientID}))
                        print "Novo cliente conectado: %d" % clientID
                        
                    elif (command == "GET_ID"):
                        clientID = message["clientid"]
                        # Se o cliente não houver sido removido, verifica disponibilidade de recurso para coleta
                        if (clientID in connectedClients):
                            clientName = connectedClients[clientID][0]
                            resourceID = selectResource()
                            # Se não houver mais recursos para coletar, finaliza cliente
                            if (resourceID == None):
                                client.send(json.dumps({"command": "FINISH"}))
                                del connectedClients[clientID]
                                closeConnection(client)
                                # Se não houver mais clientes para finalizar, finaliza servidor
                                if (not connectedClients):
                                    running = False
                                    print "Tarefa concluida, servidor finalizado."
                            # Envia ID do recurso para o cliente
                            else:
                                connectedClients[clientID][3] = resourceID
                                connectedClients[clientID][4] += 1
                                connectedClients[clientID][5] = datetime.datetime.today()
                                updateResource(resourceID, 1, 0, clientName)
                                client.send(json.dumps({"command": "GIVE_ID", "resourceid": str(resourceID)}))
                        # Se o cliente houver sido removido, sinaliza para que ele termine
                        else:
                            client.send(json.dumps({"command": "KILL"}))
                            closeConnection(client)
                        
                    elif (command == "DONE_ID"):
                        clientID = int(message["clientid"])
                        # Se o cliente não houver sido removido, atualiza banco de dados
                        if (clientID in connectedClients):
                            clientName = connectedClients[clientID][0]
                            clientResourceID = message["resourceid"]
                            clientStatus = message["status"]
                            clientAmount = message["amount"]
                            updateResource(clientResourceID, clientStatus, clientAmount, clientName)
                            client.send(json.dumps({"command": "DID_OK"}))
                        # Se o cliente houver sido removido, sinaliza para que ele termine
                        else:
                            client.send(json.dumps({"command": "KILL"}))
                            closeConnection(client)
                        
                    elif (command == "GET_STATUS"):
                        status = "\n" + (" Status (%s:%s/%s) " % (cfgAddress, cfgPort, os.getpid())).center(50, ':') + "\n\n"
                        if (connectedClients): 
                            for (clientID, clientInfo) in connectedClients.iteritems():
                                clientName = clientInfo[0]
                                clientAddress = clientInfo[1]
                                clientPid = clientInfo[2]
                                clientResourceID = clientInfo[3]
                                clientAmount = clientInfo[4]
                                clientUpdatedAt = clientInfo[5]
                                status += "  #%d %s (%s:%s/%s): %d em %s, ha %s min [%s]\n" % (clientID, clientName, clientAddress[0], clientAddress[1], clientPid, clientAmount, clientUpdatedAt.strftime("%d-%m-%Y %H:%M:%S"), "%02d:%02d" % (divmod((datetime.datetime.today() - clientUpdatedAt).seconds, 60)), clientResourceID)
                        else:
                            status += "  Nenhum cliente conectado no momento.\n"
                        status += "\n" + (" Status (%.1f%% coletado) " % (collectedResourcesPercent())).center(50, ':') + "\n"
                        client.send(json.dumps({"command": "GIVE_STATUS", "status": status}))
                        
                    elif (command == "RM_CLIENT"):
                        clientID = int(message["clientid"])
                        if (clientID in connectedClients):
                            removeClient(clientID)
                            client.send(json.dumps({"command": "RM_OK"}))
                            print "Cliente %d removido." % clientID
                        else:
                            client.send(json.dumps({"command": "RM_ERROR", "reason": "ID inexistente"}))
                            
                    elif (command == "SHUTDOWN"):
                        # Remove o soquete de escuta do servidor da lista de soquetes conectados
                        connectedSockets.remove(server)
                        # Remove todos os clientes da lista de conexão para que eles recebam
                        # uma sinalização de término no próximo contato feito com o servidor
                        for clientID in connectedClients.keys():
                            removeClient(clientID)
                        shutdown = True
                        print "Removendo todos os clientes para desligar..."
                        
                except Exception as error:
                    print "ERRO: %s" % str(error)
                    excType, excObj, excTb = sys.exc_info()
                    fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
                    print (excType, fileName, excTb.tb_lineno)
                    closeConnection(client)
            
            # Se o cliente houver fechado a conexão do outro lado,
            # fecha o soquete deste lado também
            else:
                closeConnection(client)

    # Se o servidor houver recebido um comando para desligar e todos os clientes já tiverem
    # sido desconectados, termina a execução, sinalizando sucesso para o gerenciador
    if (shutdown and len(connectedSockets) == 1):
        running = False
        connectedSockets[0].send(json.dumps({"command": "SD_OK"}))
        print "Servidor desligado manualmente."

server.close()
                