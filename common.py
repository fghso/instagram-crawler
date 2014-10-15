# -*- coding: iso-8859-1 -*-

import xmltodict


# ==================== Methods ====================
def str2bool(stringToConvert):
    if stringToConvert.lower() in ("true", "t", "yes", "y", "on", "1"): return True
    if stringToConvert.lower() in ("false", "f", "no", "n", "off", "0"): return False
    raise TypeError("The value '%s' is not considered a valid boolean in this context." % stringToConvert)

    
# ==================== Classes ====================
class ConfigurationHandler():
    def __init__(self, configFilePath):
        configFile = open(configFilePath, "r")
        configDict = xmltodict.parse(configFile.read())
        self._config = configDict["config"]
        self._setDefault()
        
    def _setDefault(self):
        self._config["global"]["connection"]["port"] = int(self._config["global"]["connection"]["port"])
        self._config["global"]["connection"]["bufsize"] = int(self._config["global"]["connection"]["bufsize"])
    
        if ("autofeed" not in self._config["global"]): self._config["global"]["autofeed"] = False
        else: self._config["global"]["autofeed"] = str2bool(self._config["global"]["autofeed"])
        
        if ("loopforever" not in self._config["server"]): self._config["server"]["loopforever"] = False
        else: self._config["server"]["loopforever"] = str2bool(self._config["server"]["loopforever"])
        
        if ("logging" not in self._config["server"]): self._config["server"]["logging"] = True
        else: self._config["server"]["logging"] = str2bool(self._config["server"]["logging"])
        
        if ("verbose" not in self._config["server"]): self._config["server"]["verbose"] = False
        else: self._config["server"]["verbose"] = str2bool(self._config["server"]["verbose"])
                
        if ("logging" not in self._config["client"]): self._config["client"]["logging"] = True
        else: self._config["client"]["logging"] = str2bool(self._config["client"]["logging"])
        
        if ("verbose" not in self._config["client"]): self._config["client"]["verbose"] = False
        else: self._config["client"]["verbose"] = str2bool(self._config["client"]["verbose"])
        
    def getConfig(self):
        return self._config
