import datetime, time, os, sys, traceback, gzip, json, arcresthelper
from arcrest import manageorg
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
layerOrder = None

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
    #TODO if this just errors when the value is not supplied
    try: 
        value = config.get(group, name)
        if value == '':
            raise configparser.NoOptionError(name, group)

        if type == 'path':
            return os.path.normpath(value)
        elif type == 'list':
            return value.split(',')
        elif type == 'bool':
            return value.lower() == 'true'
        else:
            return value
    except (configparser.NoSectionError, configparser.NoOptionError):
        if required:
            raise
        else:
            return None

def readConfig():  
    config = configparser.ConfigParser()
    config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection.cfg')))

    global featureServiceItemID
    featureServiceItemID = validateInput(config, 'Existing ItemIDs', 'featureServiceItemID', 'id', True)

    global featureCollectionItemID
    featureCollectionItemID = validateInput(config, 'Existing ItemIDs', 'featureCollectionItemID', 'id', True)

    global logPath
    logPath = validateInput(config, 'Log File Location', 'syncLog', 'path', False)

    global fgdb
    fgdb = validateInput(config, 'Data Sources', 'fgdb', 'path', True)

    global orgURL
    orgURL = validateInput(config, 'Portal Sharing URL', 'baseURL', 'url', True)

    global username
    username = validateInput(config, 'Portal Credentials', 'username', 'string', True)

    global pw
    pw = validateInput(config, 'Portal Credentials', 'pw', 'string', True)

    global maxAllowableOffset
    maxAllowableOffset = validateInput(config, 'Generalization', 'maxAllowableOffset', 'int', False)

    global layerOrder
    layerOrder = validateInput(config, 'Layers', 'order', 'list', False)

def startLogging():
    # Logging Logic
    global logPath
    if logPath is not None:  
        global starttime
        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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
    
        log.write("----------------------------" + "\n")
        log.write("----------------------------" + "\n")
        log.write("Log: " + str(d) + "\n")
        log.write("\n")
        # Start process...
        starttime = datetime.datetime.now()
        log.write("Begin Data Sync:\n")
        log.close()

def logMessage(myMessage):
    d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    if logPath is not None:
        log = open(logPath,"a")
        log.write("     " + str(d) + " - " +myMessage + "\n")
        log.close()
    print("     " + str(d) + " - " +myMessage + "\n")

def endLogging():
    # Close out the log file
    if logPath is not None:
        global starttime
        log = open(logPath,"a")
        endtime = datetime.datetime.now()
        # Process Completed...
        log.write("\n" + "Elapsed time " + str(endtime - starttime) + "\n")
        log.write("\n")
        log.close()

def logError(tb):
    tbinfo = traceback.format_tb(tb)
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + "".join(tbinfo) + "\nError Info:\n" + str(sys.exc_info()[1])
    logMessage("" + pymsg)

def deleteItem(itemID):
    org = manageorg.Administration(securityHandler=shh.securityhandler)
    usercontent = org.content.users.user(username)
    usercontent.deleteItems(items=itemID)

def getJSON(url):
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
    admin = manageorg.Administration(securityHandler=shh.securityhandler)
    content = admin.content

    #Test if item ID does not exist and return error message
    item = content.getItem(itemId=featureServiceItemID)
    if item.id is None:
        raise Exception('Unable to find feature service with ID: {}'.format(featureServiceItemID))

    global baseName
    baseName= item.title # item.name is returning None for feature service created from FGDB

    #Get the prepublished feature collection name
    fcItem = content.getItem(itemId=featureCollectionItemID)
    if fcItem.id is None:
        raise Exception('Unable to find feature collection with ID: {}'.format(featureCollectionItemID))

    global tempFCName
    tempFCName = fcItem.title + "_temp"

def uploadFGDB():
    #Search for any file geodatabse items that have a title matching the title of the Feature Service
    org = manageorg.Administration(securityHandler=shh.securityhandler)
    #search = org.search(q='owner:{} type:"File Geodatabase"'.format(username))

    #results = search['results']
    #existingGDB = next((r['id'] for r in results if r['title'] == baseName), None)
    #if existingGDB is not None:
    #    logMessage("File geodatabase {} found on the portal, deleting the item".format(baseName))
    #    deleteItem(existingGDB)
    #    logMessage("File geodatabase deleted")

    if not os.path.exists(fgdb):
        raise Exception("File GDB: {} could not be found".format(fgdb))

    logMessage("Uploading file geodatabase")

    itemParams = manageorg.ItemParameter()
    itemParams.title = baseName #this name should be derived from the fGDB
    itemParams.type = "File Geodatabase"
    itemParams.tags = "GDB"
    itemParams.typeKeywords = "Data,File Geodatabase"

    content = org.content
    usercontent = content.users.user(username)

    gdbSize = float(os.path.getsize(fgdb)) / (1024 * 1024)

    #If larger than 100 MBs we should set multipart to true
    result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb, multipart=gdbSize > 100)

    global gdbItemID
    gdbItemID = result.id

    logMessage("File geodatabase upload complete")

def updateFeatureService():
    logMessage("Updating {} feature service".format(baseName))

    org = manageorg.Administration(securityHandler=shh.securityhandler)
    content = org.content
    usercontent = content.users.user(username)

    fst = arcresthelper.featureservicetools.featureservicetools(shh)
    fs = fst.GetFeatureService(itemId=featureServiceItemID,returnURLOnly=False)

    publishParams = json.loads(getJSON(fs.url))
    publishParams['name'] = baseName
    layers = json.loads(getJSON(fs.url + "/layers"))
    publishParams['layers'] = layers['layers']
    publishParams['tables'] = layers['tables']
   
    result = usercontent.publishItem(fileType="fileGeodatabase", 
                                        publishParameters=CustomPublishParameter(publishParams),  
                                        itemId=gdbItemID, 
                                        wait=True, overwrite=True)

    logMessage("{} feature service updated".format(baseName))

def exportTempFeatureCollection():
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
    admin = manageorg.Administration(securityHandler=shh.securityhandler)
    content = admin.content
    item = content.getItem(featureCollectionItemID)

    logMessage("Updating {} feature collection".format(item.name))

    updatedFeatures = json.loads(getJSON(orgURL + "/sharing/rest/content/items/" + tempFeatureCollectionItemID + "/data"))
    
    if layerOrder is not None:
        orderedLayers = []
        layers = updatedFeatures['layers']
        for name in layerOrder:
            lyr = next((i for i in layers if i['layerDefinition']['name'] == name), None)
            if lyr is None:
                raise Exception("Layer name: {} not found in feature service, unable to update feature collection".format(name))
            orderedLayers.insert(0, lyr)
        updatedFeatures['layers'] = orderedLayers

    d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    itemParams = manageorg.ItemParameter()
    itemParams.snippet = str(d)
    item.userItem.updateItem(itemParameters=itemParams, text=json.dumps(updatedFeatures))
    logMessage("{} feature collection updated".format(item.name))

def removeTempContent():
    #Remove any temp items created during the process
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
    startLogging()
    
    securityinfo = {}
    securityinfo['username'] = username
    securityinfo['password'] = pw
    securityinfo['org_url'] = orgURL

    global shh
    shh = arcresthelper.securityhandlerhelper.securityhandlerhelper(securityinfo)

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