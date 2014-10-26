#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import json
import xmltodict
from collections import OrderedDict


accounts = open("../../../contas_clients", "r")
accounts.readline() # Descarta cabeçalho

app = OrderedDict()
#app["database"] = OrderedDict([("user", ""), ("password", ""), ("host", ""), ("name", ""), ("table", "")])
app["application"] = []

for line in accounts:
    acc = line.split(',')
    #app[acc[1]] = OrderedDict([("clientid", acc[2]), ("clientsecret", acc[3])])
    app["application"].append(OrderedDict([("@name", acc[1]), ("clientid", acc[2]), ("clientsecret", acc[3])]))
    
# Salva em JSON
jsonOuput = open("app.json", "w")    
json.dump(app, jsonOuput, indent = 4, sort_keys = True)

# Salva em XML
xmlOutput = open("app.xml", "w")
xmltodict.unparse({"instagram": app}, output=xmlOutput, encoding="ISO8859-1", pretty=True)
