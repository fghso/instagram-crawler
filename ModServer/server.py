#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import json
import xml.etree.ElementTree as ET
from datetime import datetime
import SocketServer
import threading
import logging
import argparse
import mysql.connector
from appscheduler import CrawlerParams


# ==================== Vari�veis globais ====================
# Dicion�rio que guarda uma lista de informa��es sobre cada cliente:
#   [nome do cliente, endere�o de rede, n�mero do processo (PID), 
#   ID do recurso que est� sendo coletado, quantidade de recursos j� 
#   coletados pelo cliente, data de in�cio da coleta e data da �ltima atualiza��o]
clientsInfo = {} 

# Armazena uma refer�ncia para a thread que executa o cliente
# e um evento que permite interromper sua execu��o
clientsThreads = {}

# Define pr�ximo ID a ser passado para um novo cliente
nextFreeID = 1

# Define locks para regi�es cr�ticas do c�digo
nextFreeIDLock = threading.Lock()
getIDLock = threading.Lock()


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
        # Abre conex�o com o banco de dados
        cfg = self.server.cfg
        self.mysqlConnection = mysql.connector.connect(user=cfg.DBUser, password=cfg.DBPassword, host=cfg.DBHost, database=cfg.DBName)

    def handle(self):
        cfg = self.server.cfg
        params = self.server.params
        client = self.request
        clientID = 0
        running = True
        while (running):
            try: 
                response = client.recv(cfg.Bufsize)
                
                # Se o cliente houver fechado a conex�o do outro lado, finaliza a execu��o da thread
                if (not response): 
                    #clientID = int(threading.current_thread().name)
                    if (not args.no_logging): logging.info("Cliente %d desconectou-se." % clientID)
                    if (args.verbose): print "Cliente %d desconectou-se." % clientID
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
                    #threading.current_thread().name = clientID
                    clientName = message["name"]
                    #clientAddress = (socket.gethostbyaddr(client.getpeername()[0])[0], client.getpeername()[1])
                    clientAddress = client.getpeername()
                    clientPid = message["processid"]
                    clientsInfo[clientID] = [clientName, clientAddress, clientPid, None, 0, datetime.now(), None]
                    clientsThreads[clientID] = (threading.current_thread(), threading.Event())
                    client.send(json.dumps({"command": "GIVE_LOGIN", "clientid": clientID}))
                    if (not args.no_logging): logging.info("Novo cliente conectado: %d" % clientID)
                    if (args.verbose): print "Novo cliente conectado: %d" % clientID
                
                elif (command == "GET_ID"):
                    #clientID = message["clientid"]
                    clientStopEvent = clientsThreads[clientID][1]
                    # Se o cliente n�o houver sido removido, verifica disponibilidade de recurso para coleta
                    if (not clientStopEvent.is_set()):
                        clientName = clientsInfo[clientID][0]
                        with getIDLock:
                            resourceID = self.selectResource()
                            if (resourceID): self.updateResource(resourceID, 1, 0, clientName)
                        # Se houver recurso dispon�vel, envia o ID para o cliente
                        if (resourceID):
                            clientsInfo[clientID][3] = resourceID
                            clientsInfo[clientID][4] += 1
                            clientsInfo[clientID][6] = datetime.now()
                            client.send(json.dumps({"command": "GIVE_ID", "resourceid": str(resourceID), "params": params.getParams()}))
                        # Se n�o houver mais recursos para coletar, finaliza cliente
                        else:
                            client.send(json.dumps({"command": "FINISH"}))
                            del clientsInfo[clientID]
                            running = False
                            # Se n�o houver mais clientes para finalizar, finaliza servidor
                            if (not clientsInfo):
                                self.server.shutdown()
                                if (not args.no_logging): logging.info("Tarefa concluida, servidor finalizado.")
                                if (args.verbose): print "Tarefa concluida, servidor finalizado."
                    # Se o cliente houver sido removido, sinaliza para que ele termine
                    else:
                        client.send(json.dumps({"command": "KILL"}))
                        del clientsInfo[clientID]
                        if (not args.no_logging): logging.info("Cliente %d removido." % clientID)
                        if (args.verbose): print "Cliente %d removido." % clientID
                        running = False
                    
                elif (command == "DONE_ID"):
                    #clientID = int(message["clientid"])
                    clientName = clientsInfo[clientID][0]
                    clientResourceID = message["resourceid"]
                    clientStatus = message["status"]
                    clientAnnotation = message["annotation"]
                    self.updateResource(clientResourceID, clientStatus, clientAnnotation, clientName)
                    client.send(json.dumps({"command": "DID_OK"}))
                    
                elif (command == "GET_STATUS"):
                    status = "\n" + (" Status (%s:%s/%s) " % (cfg.Address, cfg.Port, os.getpid())).center(50, ':') + "\n\n"
                    if (clientsInfo): 
                        for (ID, clientInfo) in clientsInfo.iteritems():
                            clientAlive = (" " if clientsThreads[ID][0].is_alive() else "+")
                            clientName = clientInfo[0]
                            clientAddress = clientInfo[1]
                            clientPid = clientInfo[2]
                            clientResourceID = clientInfo[3]
                            clientAmount = clientInfo[4]
                            clientStartTime = clientInfo[5]
                            clientUpdatedAt = clientInfo[6]
                            elapsedTime = datetime.now() - clientStartTime
                            elapsedMinSec = divmod(elapsedTime.seconds, 60)
                            elapsedHoursMin = divmod(elapsedMinSec[0], 60)
                            status += "  #%d %s %s (%s:%s/%s): %s desde %s [%d coletado(s) em %s]\n" % (ID, clientAlive, clientName, clientAddress[0], clientAddress[1], clientPid, clientResourceID, clientUpdatedAt.strftime("%d/%m/%Y %H:%M:%S"), clientAmount, "%02dh%02dm%02ds" % (elapsedHoursMin[0],  elapsedHoursMin[1], elapsedMinSec[1]))
                    else:
                        status += "  Nenhum cliente conectado no momento.\n"
                    status += "\n" + (" Status (%.1f%% coletado) " % (self.collectedResourcesPercent())).center(50, ':') + "\n"
                    client.send(json.dumps({"command": "GIVE_STATUS", "status": status}))
                    running = False
                    
                elif (command == "RM_CLIENT"):
                    ID = int(message["clientid"])
                    if (ID in clientsThreads):
                        # Se a thread estava ativa, sinaliza para que ela termine de maneira segura e aguarda
                        if (clientsThreads[ID][0].is_alive()):
                            clientsThreads[ID][1].set()
                            while (clientsThreads[ID][0].is_alive()): pass
                        # Se a thread n�o estava ativa, marca o �ltimo ID solicitado como n�o 
                        # coletado para que a coleta seja refeita, garantindo a consist�ncia
                        else:
                            clientName = clientsInfo[ID][0]
                            clientResourceID = clientsInfo[ID][3]
                            self.updateResource(clientResourceID, 0, None, clientName)
                            if (not args.no_logging): logging.info("Cliente %d removido." % ID)
                            if (args.verbose): print "Cliente %d removido." % ID
                        del clientsThreads[ID]
                        client.send(json.dumps({"command": "RM_OK"}))
                    else:
                        client.send(json.dumps({"command": "RM_ERROR", "reason": "ID inexistente"}))
                    running = False
                        
                elif (command == "SHUTDOWN"):
                    # Sinaliza para que todos os clientes ativos terminem e marca recursos dos clientes inativos
                    # como n�o coletados. Em seguida, desliga o servidor
                    if (not args.no_logging): logging.info("Removendo todos os clientes para desligar...")
                    if (args.verbose): print "Removendo todos os clientes para desligar..."
                    for ID in clientsThreads.keys():
                        if (clientsThreads[ID][0].is_alive()):
                            clientsThreads[ID][1].set()
                        else:
                            clientName = clientsInfo[ID][0]
                            clientResourceID = clientsInfo[ID][3]
                            self.updateResource(clientResourceID, 0, None, clientName)
                    while (threading.active_count() > 2): pass
                    self.server.shutdown()    
                    client.send(json.dumps({"command": "SD_OK"}))
                    if (not args.no_logging): logging.info("Servidor desligado manualmente.")
                    if (args.verbose): print "Servidor desligado manualmente."
                    running = False
            
            except Exception as error:
                #clientID = int(threading.current_thread().name)
                if (not args.no_logging): logging.exception("Excecao no processamento da requisicao do cliente %d. Thread '%s' abortada." % (clientID, threading.current_thread().name))
                if (args.verbose): 
                    print "ERRO: %s" % str(error)
                    excType, excObj, excTb = sys.exc_info()
                    fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
                    print (excType, fileName, excTb.tb_lineno)
                running = False
            
    # Fun��es de intera��o com o banco de dados
    def selectResource(self):
        query = "SELECT resource_id FROM " + self.server.cfg.DBTable + " WHERE status IS NULL OR status = 0 ORDER BY resources_pk LIMIT 1"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query)
        resource = cursor.fetchone()
        return resource[0] if (resource) else resource
        
    def updateResource(self, resourceID, status, annotation, crawler):
        query = "UPDATE " + self.server.cfg.DBTable + " SET status = %s, annotation = %s, crawler = %s WHERE resource_id = %s"
        cursor = self.mysqlConnection.cursor()
        cursor.execute(query, (status, annotation, crawler, resourceID))
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
        
        
class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    def __init__(self, configuration, crawlerParameters, RequestHandlerClass):
        self.cfg = configuration
        self.params = crawlerParameters
        SocketServer.TCPServer.__init__(self, (self.cfg.Address, self.cfg.Port), RequestHandlerClass)
        
        
# ==================== Programa principal ====================

# Analisa argumentos
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="mostra esta mensagem de ajuda e sai")
parser.add_argument("-v", "--verbose", action="store_true", help="exibe as mensagens de log na tela")
parser.add_argument("--no-logging", action="store_true", help="desabilita logging")
args = parser.parse_args()

# Carrega configura��es
configuration = ServerConfig(args.configFilePath)

# Carrega par�metros dos clientes
crawlerParameters = CrawlerParams()

# Configura logging
if (not args.no_logging):
    logging.basicConfig(format="%(asctime)s %(module)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename="server[%s%s].log" % (configuration.Address, configuration.Port), filemode="w", level=logging.INFO)

# Inicia servidor
if (not args.no_logging): logging.info("Servidor pronto. Aguardando conexoes...")
if (args.verbose): print "Servidor pronto. Aguardando conexoes..."
server = ThreadedTCPServer(configuration, crawlerParameters, ServerHandler)
server.serve_forever()
                