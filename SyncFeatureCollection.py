"""
-------------------------------------------------------------------------------
 | Copyright 2016 Esri
 |
 | Licensed under the Apache License, Version 2.0 (the "License");
 | you may not use this file except in compliance with the License.
 | You may obtain a copy of the License at
 |
 |    http://www.apache.org/licenses/LICENSE-2.0
 |
 | Unless required by applicable law or agreed to in writing, software
 | distributed under the License is distributed on an "AS IS" BASIS,
 | WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 | See the License for the specific language governing permissions and
 | limitations under the License.
 ------------------------------------------------------------------------------
 """
import datetime, time, os, sys, traceback, gzip, json, arcresthelper
from arcrest import manageorg
from arcrest.agol import FeatureLayer
from io import BytesIO

try:
    import http.client as client
    import urllib.parse as parse
    from urllib.request import urlopen as urlopen
    from urllib.request import Request as request
    from urllib.parse import urlencode as encode
    import configparser as configparser
# py2
except ImportError:
    import httplib as client
    from urllib2 import urlparse as parse
    from urllib2 import urlopen as urlopen
    from urllib2 import Request as request
    from urllib import urlencode as encode
    import ConfigParser as configparser
    unicode = str

logPath = None
starttime = None
tempFeatureCollectionItemID = None
gdbItemID = None
shh = None
layerMapping = None

class CustomPublishParameter():
    _value = None
    #----------------------------------------------------------------------
    def __init__(self,
                 value,
                 ):
        """Constructor"""
        self._value = value
    #----------------------------------------------------------------------
    @property
    def value(self):
        return self._value

def validateInput(config, group, name, type, required):
    """Validates and returns the correspoinding value defined in the config.

    Keyword arguments:
    config - the instance of the configparser
    group - the name of the group containing the property
    name - the name of the property to get that value for
    type - the type of property, 'path', 'mapping' 'bool', otherwise return the raw string
    required - if the option is required and none is found than raise an exception
    """
    try: 
        value = config.get(group, name)
        if value == '':
            raise configparser.NoOptionError(name, group)

        if type == 'path':
            return os.path.normpath(value)
        elif type == 'mapping':
            return list(v.split(',') for v in value.split(';'))
        elif type == 'bool':
            return value.lower() == 'true'
        else:
            return value
    except (configparser.NoSectionError, configparser.NoOptionError):
        if required:
            raise
        elif type == 'bool':
            return False
        else:
            return None

def readConfig():
    """Read the config and set global variables used in the script."""  
    config = configparser.ConfigParser()
    config.readfp(open(os.path.join(sys.path[0], 'SyncFeatureCollection.cfg')))

    global logPath
    logPath = validateInput(config, 'Log File', 'path', 'path', False)

    global isVerbose
    isVerbose = validateInput(config, 'Log File', 'isVerbose', 'bool', False)
    
    startLogging()

    global featureServiceItemID
    featureServiceItemID = validateInput(config, 'Existing ItemIDs', 'featureServiceItemID', 'id', True)

    global featureCollectionItemID
    featureCollectionItemID = validateInput(config, 'Existing ItemIDs', 'featureCollectionItemID', 'id', True)

    global fgdb
    fgdb = validateInput(config, 'Data Sources', 'fgdb', 'path', True)

    global orgURL
    orgURL = validateInput(config, 'Portal Sharing URL', 'baseURL', 'url', True)

    global tokenURL
    tokenURL = validateInput(config, 'Portal Sharing URL', 'tokenURL', 'url', False)

    global username
    username = validateInput(config, 'Portal Credentials', 'username', 'string', True)

    global pw
    pw = validateInput(config, 'Portal Credentials', 'pw', 'string', True)

    global maxAllowableOffset
    maxAllowableOffset = validateInput(config, 'Generalization', 'maxAllowableOffset', 'int', False)

    global layerMapping
    layerMapping = validateInput(config, 'Layers', 'nameMapping', 'mapping', False)

def startLogging():
    """If a log file is specified in the config, create it if it doesn't exist and write the start time of the run.""" 
    global starttime
    starttime = datetime.datetime.now()
    
    global logPath
    if logPath is not None:  
        isFile = os.path.isfile(logPath)

        logfileLocation = os.path.abspath(os.path.dirname(logPath))
        if not os.path.exists(logfileLocation):
            os.makedirs(logfileLocation)

        if isFile:
            path = logPath
        else:
            path = os.path.join(logfileLocation, "SyncLog.txt")
       
        logPath = path
        log = open(path,"a")
        d = starttime.strftime('%Y-%m-%d %H:%M:%S')
        log.write("----------------------------" + "\n")
        log.write("Begin Data Sync: " + str(d) + "\n")
        log.close()

def logMessage(myMessage, isError=False):
    """Log a new message and print to the python output.
    
    Keyword arguments:
    myMessage - the message to log
    isError - indicates if the message is an error, used to log even when verbose logging is disabled
    """
    d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if logPath is not None and (isVerbose or isError):
        log = open(logPath,"a")
        log.write("     " + str(d) + " - " +myMessage + "\n")
        log.close()
    print("     " + str(d) + " - " +myMessage + "\n")

def endLogging():
    """If a log file is specified in the config write the elapsed time."""
    if logPath is not None:
        global starttime
        log = open(logPath,"a")
        endtime = datetime.datetime.now()
        # Process Completed...
        log.write("Elapsed Time: " + str(endtime - starttime) + "\n")
        log.close()

def logError(tb):
    """Log an error message.
    
    Keyword arguments:
    tb - the traceback from the exception"""
    tbinfo = traceback.format_tb(tb)
    tbinfo = traceback.format_tb(tb)
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + "".join(tbinfo) + "\nError Info:\n" + str(sys.exc_info()[1])
    logMessage(pymsg, True)

def deleteItem(itemID):
    """Delete an item from the organization.
    
    Keyword arguments:
    itemID - the id of the item to delete"""
    org = manageorg.Administration(securityHandler=shh.securityhandler)
    usercontent = org.content.users.user(username)
    usercontent.deleteItems(items=itemID)

def getJSON(url):
    """Get the json defintion of a feature service or feature collection.
    
    Keyword arguments:
    url - the url of the item."""
    request_parameters = {'f' : 'json','token' : shh.securityhandler.token }
    headers = {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',}

    req = request(url, encode(request_parameters).encode('UTF-8'), headers)
    req.add_header('Accept-encoding', 'gzip')
    response = urlopen(req)

    if response.info().get('Content-Encoding') == 'gzip':
        buf = BytesIO(response.read())
        with gzip.GzipFile(fileobj=buf) as gzip_file:
            response_bytes = gzip_file.read()
    else:
        response_bytes = response.read()
    return response_bytes.decode('UTF-8')

def getPublishedItems():
    """Validates the feature service and feature collection exist and sets global variables."""
    admin = manageorg.Administration(securityHandler=shh.securityhandler)
    content = admin.content

    #Test if item ID does not exist and return error message
    item = content.getItem(itemId=featureServiceItemID)
    if item.id is None:
        raise Exception('Unable to find feature service with ID: {}'.format(featureServiceItemID))

    global baseName
    baseName = item.title 

    #Get the prepublished feature collection name
    fcItem = content.getItem(itemId=featureCollectionItemID)
    if fcItem.id is None:
        raise Exception('Unable to find feature collection with ID: {}'.format(featureCollectionItemID))

    global tempFCName
    tempFCName = fcItem.title + "_temp"

def uploadFGDB():
    """Uploads the file geodatabase to the portal."""
    org = manageorg.Administration(securityHandler=shh.securityhandler)

    if not os.path.exists(fgdb):
        raise Exception("File GDB: {} could not be found".format(fgdb))

    logMessage("Uploading file geodatabase")

    itemParams = manageorg.ItemParameter()
    itemParams.title = baseName #this name should be derived from the fGDB
    itemParams.type = "File Geodatabase"
    itemParams.tags = "SyncFeatureCollection"
    itemParams.typeKeywords = "Data,File Geodatabase"

    content = org.content
    usercontent = content.users.user(username)

    gdbSize = float(os.path.getsize(fgdb)) / (1024 * 1024)
    gdbName = os.path.basename(fgdb)

    #If larger than 100 MBs we should set multipart to true
    try:
        result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb, multipart=gdbSize > 100)
    except:
        search = org.search(q='SyncFeatureCollection owner:{0} type:"File Geodatabase"'.format(username))

        results = search['results']
        existingGDB = next((r['id'] for r in results if (r['name'] == gdbName and "SyncFeatureCollection" in r['tags'])), None)
        if existingGDB is not None:
            logMessage("File geodatabase {} found on the portal, deleting the item".format(gdbName))
            deleteItem(existingGDB)
            logMessage("File geodatabase deleted")

        result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb, multipart=gdbSize > 100)
    
    global gdbItemID
    gdbItemID = result.id

    logMessage("File geodatabase upload complete")

def updateFeatureService():
    """Overwrites the feature service using the uploaded file geodatabase."""
    logMessage("Updating {} feature service".format(baseName))

    org = manageorg.Administration(securityHandler=shh.securityhandler)
    content = org.content
    usercontent = content.users.user(username)

    item = content.getItem(itemId=featureServiceItemID)
    url = item.url

    publishParams = json.loads(getJSON(url))
    publishParams['name'] = os.path.basename(os.path.dirname(url))
    layersJSON = getJSON(url + "/layers")    
    layers = json.loads(layersJSON)

    complexRenderers = {} # Overwriting a feature service from a FGDB does not support complext renderers
    for layer in layers['layers']:
        if 'drawingInfo' in layer:
            if 'renderer' in layer['drawingInfo']:
                if 'type' in layer['drawingInfo']['renderer']:
                    if layer['drawingInfo']['renderer']['type'] != 'simple':
                        complexRenderers[layer['id']] = layer['drawingInfo']
                        layer['drawingInfo'] = ""             

    publishParams['layers'] = layers['layers']
    publishParams['tables'] = layers['tables']
   
    if layerMapping is not None: # Name of the layer must match the name of the feature class in the GDB
        for map in layerMapping:
            lyr = next((i for i in publishParams['layers'] if i['name'] == map[0]), None)
            if lyr is not None:
                lyr['name'] = map[1]

    ex = None
    try:
        result = usercontent.publishItem(fileType="fileGeodatabase", 
                                            publishParameters=CustomPublishParameter(publishParams),  
                                            itemId=gdbItemID, 
                                            wait=True, overwrite=True)
  
        logMessage("{} feature service updated".format(baseName))
    except Exception as ex:
        pass

    for id in complexRenderers: # Set the renderer definition back on the layer after overwrite completes
        fl = FeatureLayer(url=url + "/" + str(id),
        securityHandler=shh.securityhandler,
        initialize=True)

        logMessage("Updating {} drawing info".format(fl.name))
        adminFl = fl.administration
        succeed = False
        for i in range(3):
            try:
                adminFl.updateDefinition({'drawingInfo':complexRenderers[id]})
                succeed = True
                break
            except:
                continue

        if succeed:
            logMessage("{} drawing info updated".format(fl.name))
        else:
            logMessage("{} drawing info failed to update".format(fl.name))

    if ex is not None:
        logMessage("{} feature service failed to update".format(baseName))
        raise ex

def exportTempFeatureCollection():
    """Exports the feature service to a temporary feature collection."""
    logMessage("Exporting " + baseName + " to a temporary feature collection")    

    org = manageorg.Administration(securityHandler=shh.securityhandler)
    content = org.content
    usercontent = content.users.user(username)
    expParams = {}
    if maxAllowableOffset is not None:
       expParams.update({"maxAllowableOffset":maxAllowableOffset})
 
    global tempFeatureCollectionItemID
    result = usercontent.exportItem(title=tempFCName,
                                itemId=featureServiceItemID,
                                exportFormat="feature collection",
                                exportParameters=expParams,
                                wait=False)
    jobID = result[0]
    userItem = result[1]
    tempFeatureCollectionItemID = userItem.id

    status = "processing"
    while status != "completed":
        status = userItem.status(jobId=jobID, jobType="export")
        if status['status'].lower() == 'failed':
            raise Exception("Could not export item: {}".format(tempFeatureCollectionItemID))
        elif status['status'].lower() == 'completed':
            break
        time.sleep(2)  
              
    logMessage("Temp feature collection created")                    

def updateFeatureCollection():
    """Updates the productiong feature collection using the features in the temporary feature collection."""
    admin = manageorg.Administration(securityHandler=shh.securityhandler)
    content = admin.content
    item = content.getItem(featureCollectionItemID)

    logMessage("Updating {} feature collection".format(item.name))

    updatedFeatures = json.loads(getJSON(orgURL + "/sharing/rest/content/items/" + tempFeatureCollectionItemID + "/data"))

    d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    itemParams = manageorg.ItemParameter()
    itemParams.snippet = str(d)
    item.userItem.updateItem(itemParameters=itemParams, text=json.dumps(updatedFeatures))
    logMessage("{} feature collection updated".format(item.name))

def removeTempContent():
    """Remove the temporary file geodatabase and feature collection from the portal."""
    if shh is not None:
        org = manageorg.Administration(securityHandler=shh.securityhandler)
        content = org.content
        usercontent = content.users.user(username)
        
        if gdbItemID is not None:
            gdbItem = content.getItem(itemId=gdbItemID)
            if gdbItem.id is not None:
                deleteItem(gdbItem.id)
                logMessage("File geodatabase deleted")

        if tempFeatureCollectionItemID is not None:
            fcItem = content.getItem(itemId=tempFeatureCollectionItemID)
            if fcItem.id is not None:
                deleteItem(fcItem.id)
                logMessage("Temp feature collection deleted")

def main():
    readConfig()
    
    securityinfo = {}
    securityinfo['username'] = username
    securityinfo['password'] = pw
    securityinfo['org_url'] = orgURL
    securityinfo['token_url'] = tokenURL

    global shh
    shh = arcresthelper.securityhandlerhelper.securityhandlerhelper(securityinfo)

    if not shh.securityhandler.valid:
        raise Exception("Unable to connect to specified portal. Please verify you are passing in your correct portal url, username and password.")

    getPublishedItems()  
    uploadFGDB()
    updateFeatureService()
    exportTempFeatureCollection()
    updateFeatureCollection()
    
if __name__ == "__main__":
    try:
        main()
    except:
        logError(sys.exc_info()[2]) 
    finally:
        removeTempContent()
        endLogging()  
