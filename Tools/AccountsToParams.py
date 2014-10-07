#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import json
import xmltodict
from collections import OrderedDict


accounts = open("../../../contas_clients", "r")
accounts.readline() # Descarta cabeçalho

params = OrderedDict()
params["database"] = OrderedDict([("user", ""), ("password", ""), ("host", ""), ("name", "")])
params["instagram"] = OrderedDict()
#params["instagram"]["application"] = []

for line in accounts:
    acc = line.split(',')
    params["instagram"][acc[1]] = OrderedDict([("clientid", acc[2]), ("clientsecret", acc[3])])
    #params["instagram"]["application"].append(OrderedDict([("@name", acc[1]), ("clientid", acc[2]), ("clientsecret", acc[3])]))
    
# Salva em JSON
jsonOuput = open("params.json", "w")    
json.dump(params, jsonOuput, indent = 4, sort_keys = True)

# Salva em XML
xmlOutput = open("params.xml", "w")
xmltodict.unparse({"params": params}, output=xmlOutput, encoding="ISO8859-1", pretty = True)
