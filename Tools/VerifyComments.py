#!/usr/bin/python
# -*- coding: iso-8859-1 -*-

import os
import json


dirName = "data/comments2"
    
for fileName in os.listdir(dirName):
    fileObj = open(dirName + fileName, "r")
    #input = json.load(fileObj)
    input = fileObj.read()
    print input
    fileObj.close()
    break
        
        
