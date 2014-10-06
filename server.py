#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import SocketServer
import threading
import json
import xml.etree.ElementTree as ET
import datetime
import mysql.connector


# ==================== Variáveis globais ====================
# Dicionário que guarda uma lista de informações sobre cada cliente:
#   [nome do cliente, endereço de rede, número do processo (PID), 
#   ID do recurso que está sendo coletado, quantidade de recursos já 
#   coletados pelo cliente, data da última atualização]
clientsInfo = {} 

# Armazena uma referência para a thread que executa o cliente
# e um evento que permite interromper sua execução
clientsThreads = {}

nextFreeID = 1
nextFreeIDLock = threading.Lock()

# ==================== Classes ====================
class ServerConfig():
    Address = Port = Bufsize = DBUser = DBPassword = DBHost = DBName = DBTable = ""
    
    def __init__(self, configFile):
        config = ET.parse(configFile)
        ServerConfig.Address = config.findtext("./connection/address")
        ServerConfig.Port = int(config.findtext("./connection/port"))
        ServerConfig.Bufsize = int(config.findtext("./connection/bufsize"))
        ServerConfig.DBUser = config.findtext("./database/user")
        ServerConfig.DBPassword = config.findtext("./database/password")
        ServerConfig.DBHost = config.findtext("./database/host")
        ServerConfig.DBName = config.findtext("./database/name")
        ServerConfig.DBTable = config.findtext("./database/table")

        
class ServerHandler(SocketServer.BaseRequestHandler):
    def setup(self):
        # Abre conexão com o banco de dados
        cfg = self.server.cfg
        self.mysqlConnection = mysql.connector.connect(user=cfg.DBUser, password=cfg.DBPassword, host=cfg.DBHost, database=cfg.DBName)

    def handle(self):
        cfg = self.server.cfg
        client = self.request
        running = True
        while (running):
            try: 
                response = client.recv(cfg.Bufsize)
                
                # Se o cliente houver fechado a conexão do outro lado, finaliza a execução da thread
                if (not response): 
                    running = False
                    continue

                message = json.loads(response)
                command = message["command"]
                
                if (command == "GET_LOGIN"):
                    #clientID = int(threading.current_thread().name.split('-')[1])
                    with nextFreeIDLock:
                        global nextFreeID
                        clientID = nextFreeID
                        nextFreeID += 1
                    clientName = message["name"]
                    #clientAddress = (socket.gethostbyaddr(client.getpeername()[0])[0], client.getpeername()[1])
                    clientAddress = client.getpeername()
                    clientPid = message["processid"]
                    clientsInfo[clientID] = [clientName, clientAddress, clientPid, None, 0, None]
                    clientsThreads[clientID] = (threading.current_thread(), threading.Event())
                    client.send(json.dumps({"command": "GIVE_LOGIN", "clientid": clientID, "clientparams": self.getClientParams()}))
                    print "Novo cliente conectado: %d" % clientID
                
                elif (command == "GET_ID"):
                    clientID = message["clientid"]
                    clientStopEvent = clientsThreads[clientID][1]
                    # Se o cliente não houver sido removido, verifica disponibilidade de recurso para coleta
                    if (not clientStopEvent.is_set()):
                        clientName = clientsInfo[clientID][0]
                        resourceID = self.selectResource()
                        # Se não houver mais recursos para coletar, finaliza cliente
                        if (resourceID == None):
                            client.send(json.dumps({"command": "FINISH"}))
                            del clientsInfo[clientID]
                            running = False
                            # Se não houver mais clientes para finalizar, finaliza servidor
                            if (not clientsInfo):
                                self.server.shutdown()
                                print "Tarefa concluida, servidor finalizado."
                        # Envia ID do recurso para o cliente
                        else:
                            clientsInfo[clientID][3] = resourceID
                            clientsInfo[clientID][4] += 1
                            clientsInfo[clientID][5] = datetime.datetime.today()
                            self.updateResource(resourceID, 1, 0, clientName)
                            client.send(json.dumps({"command": "GIVE_ID", "resourceid": str(resourceID)}))
                    # Se o cliente houver sido removido, sinaliza para que ele termine
                    else:
                        client.send(json.dumps({"command": "KILL"}))
                        self.removeClient(clientID)
                        running = False
                        print "Cliente %d removido." % clientID
                    
                elif (command == "DONE_ID"):
                    clientID = int(message["clientid"])
                    clientStopEvent = clientsThreads[clientID][1]
                    # Se o cliente não houver sido removido, atualiza banco de dados
                    if (not clientStopEvent.is_set()):
                        clientName = clientsInfo[clientID][0]
                        clientResourceID = message["resourceid"]
                        clientStatus = message["status"]
                        clientAmount = message["amount"]
                        self.updateResource(clientResourceID, clientStatus, clientAmount, clientName)
                        client.send(json.dumps({"command": "DID_OK"}))
                    # Se o cliente houver sido removido, sinaliza para que ele termine
                    else:
                        client.send(json.dumps({"command": "KILL"}))
                        self.removeClient(clientID)
                        running = False
                        print "Cliente %d removido." % clientID
                    
                elif (command == "GET_STATUS"):
                    status = "\n" + (" Status (%s:%s/%s) " % (cfg.Address, cfg.Port, os.getpid())).center(50, ':') + "\n\n"
                    if (clientsInfo): 
                        for (clientID, clientInfo) in clientsInfo.iteritems():
                            clientName = clientInfo[0]
                            clientAddress = clientInfo[1]
                            clientPid = clientInfo[2]
                            clientResourceID = clientInfo[3]
                            clientAmount = clientInfo[4]
                            clientUpdatedAt = clientInfo[5]
                            elapsedTime = datetime.datetime.today() - clientUpdatedAt
                            elapsedMinSec = divmod(elapsedTime.seconds, 60)
                            elapsedHours = (elapsedMinSec[0] // 60,)
                            status += "  #%d %s (%s:%s/%s): %s solicitado em %s [%d coletado(s) em %s de execucao]\n" % (clientID, clientName, clientAddress[0], clientAddress[1], clientPid, clientResourceID, clientUpdatedAt.strftime("%d-%m-%Y %H:%M:%S"), clientAmount, "%02dh%02dmin%02dseg" % (elapsedHours + elapsedMinSec))
                    else:
                        status += "  Nenhum cliente conectado no momento.\n"
                    status += "\n" + (" Status (%.1f%% coletado) " % (self.collectedResourcesPercent())).center(50, ':') + "\n"
                    client.send(json.dumps({"command": "GIVE_STATUS", "status": status}))
                    
                elif (command == "RM_CLIENT"):
                    clientID = int(message["clientid"])
                    if (clientID in clientsThreads):
                        clientsThreads[clientID][1].set()
                        while (clientsThreads[clientID][0].is_alive()): pass
                        del clientsThreads[clientID]
                        client.send(json.dumps({"command": "RM_OK"}))
                    else:
                        client.send(json.dumps({"command": "RM_ERROR", "reason": "ID inexistente"}))
                        
                elif (command == "SHUTDOWN"):
                    # Sinaliza para que todos os clientes terminem e então desliga o servidor 
                    print "Removendo todos os clientes para desligar..."
                    for clientID in clientsThreads.keys():
                        clientsThreads[clientID][1].set()
                    while (threading.active_count() > 2): pass
                    self.server.shutdown()    
                    client.send(json.dumps({"command": "SD_OK"}))
                    print "Servidor desligado manualmente."
            
            except Exception as error:
                print "ERRO: %s" % str(error)
                excType, excObj, excTb = sys.exc_info()
                fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
                print (excType, fileName, excTb.tb_lineno)
                running = False
            
    # Funções de interação com o banco de dados
    def selectResource(self):
        #query = "SELECT resource_id FROM " + self.server.cfg.DBTable + " WHERE status IS NULL OR status = 0 ORDER BY rand() LIMIT 1"
        query = "SELECT resource_id FROM " + self.server.cfg.DBTable + " WHERE status = 2 ORDER BY rand() LIMIT 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resource = cursor.fetchone()
        return resource[0] if (resource != None) else resource
        
    def updateResource(self, resourceID, status, amount, crawler):
        query = "UPDATE " + self.server.cfg.DBTable + " SET status = %s, amount = %s, crawler = %s WHERE resource_id = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (status, amount, crawler, resourceID))
        self.mysqlConnection.commit()
        
    def collectedResourcesPercent(self):
        query = "SELECT count(resource_id) FROM " + self.server.cfg.DBTable
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resourcesTotal = float(cursor.fetchone()[0])
        query = "SELECT count(resource_id) FROM " + self.server.cfg.DBTable + " WHERE status IS NOT NULL AND status != 0 AND status != 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resourcesCollected = float(cursor.fetchone()[0])
        return (resourcesCollected / resourcesTotal) * 100
        
    # Funções auxiliares
    def getClientParams(self):
        # Este método pode ser usado para enviar parâmetros ao coletor, caso necessário. Os parâmetros devem ser nomeados 
        # e estarem contidos num dicionário. Por padrão, retorna apenas um dicionário vazio
        return {}
    
    def removeClient(self, clientID):
        clientName = clientsInfo[clientID][0]
        clientResourceID = clientsInfo[clientID][3]
        self.updateResource(clientResourceID, 0, 0, clientName)
        del clientsInfo[clientID]
        
        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, configuration, RequestHandlerClass):
        self.cfg = configuration
        SocketServer.TCPServer.__init__(self, (self.cfg.Address, self.cfg.Port), RequestHandlerClass)
        
        
# ==================== Programa principal ====================
try:
    CONFIGFILE = sys.argv[1]
except:
    sys.exit("usage: %s configFilePath" % sys.argv[0])
    
# Carrega configurações
configuration = ServerConfig(CONFIGFILE)

# Inicia servidor
print "Servidor pronto. Aguardando conexoes..."
server = ThreadedTCPServer(configuration, ServerHandler)
server.serve_forever()
                