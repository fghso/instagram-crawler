#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import sys
import os
import json
import logging
import argparse
import common


# Analyse arguments
parser = argparse.ArgumentParser(add_help=False)
parser.add_argument("configFilePath")
parser.add_argument("-h", "--help", action="help", help="show this help message and exit")
parser.add_argument("-v", "--verbose", type=common.str2bool, metavar="on/off", help="enable/disable log messages on screen")
parser.add_argument("-g", "--logging", type=common.str2bool, metavar="on/off", help="enable/disable logging on file")
args = parser.parse_args()

# Add directory of the configuration file to sys.path
configFileDir = os.path.dirname(os.path.abspath(args.configFilePath))
sys.path = [configFileDir] + sys.path

import crawler

# Load configurations
config = common.loadConfig(args.configFilePath)
if (args.verbose is not None): config["client"]["verbose"] = args.verbose
if (args.logging is not None): config["client"]["logging"] = args.logging

# Get an instance of the crawler
crawlerObject = crawler.Crawler()

# Get client ID
processID = os.getpid()
server = common.NetworkHandler()
server.connect(config["global"]["connection"]["address"], config["global"]["connection"]["port"])
server.send({"command": "GET_LOGIN", "name": crawlerObject.getName(), "processid": processID})
message = server.recv()
clientID = message["clientid"]

# Configure logging
if (config["client"]["logging"]):
    logging.basicConfig(format="%(asctime)s %(module)s %(levelname)s: %(message)s", datefmt="%d/%m/%Y %H:%M:%S", 
                        filename="client%s[%s%s].log" % (clientID, config["global"]["connection"]["address"], config["global"]["connection"]["port"]), filemode="w", level=logging.DEBUG)
    logging.info("Connected to server with ID %s " % clientID)
if (config["client"]["verbose"]): print "Connected to server with ID %s " % clientID

# Execute collection
getNewID = True
while (True):
    try:
        if (getNewID): 
            server.send({"command": "GET_ID"})
            getNewID = False
    
        message = server.recv()
        command = message["command"]
        
        if (command == "GIVE_ID"):
            # Call crawler with resource ID and parameters received from the server
            resourceID = message["resourceid"]
            filters = message["filters"]
            crawlerResponse = crawlerObject.crawl(resourceID, config["client"]["logging"], filters)
            
            # Tell server that the collection of the resource has been finished
            server.send({"command": "DONE_ID", "resourceid": resourceID, "resourceinfo": crawlerResponse[0]})
            
        elif (command == "DID_OK"):
            # If feedback is enabled and crawler returns new resources to be stored, send them to server
            if (config["global"]["feedback"] and crawlerResponse[1]):
                server.send({"command": "STORE_IDS", "resourceslist": crawlerResponse[1]})
            else: getNewID = True
                
        elif (command == "STORE_OK"):
            getNewID = True
            
        elif (command == "FINISH"):
            if (config["client"]["logging"]): logging.info("Task done, client finished.")
            if (config["client"]["verbose"]): print "Task done, client finished."
            break
            
        elif (command == "KILL"):
            if (config["client"]["logging"]): logging.info("Client removed by the server.")
            if (config["client"]["verbose"]): print "Client removed by the server."
            break
            
    except Exception as error:
        if (config["client"]["logging"]): logging.exception("Exception while processing data. Execution aborted.")
        if (config["client"]["verbose"]):
            print "ERROR: %s" % str(error)
            excType, excObj, excTb = sys.exc_info()
            fileName = os.path.split(excTb.tb_frame.f_code.co_filename)[1]
            print (excType, fileName, excTb.tb_lineno)
        break

server.close()
