import datetime, os, sys, time, traceback, gzip, json, arcrest, arcresthelper
from arcrest import manageorg
from arcresthelper import featureservicetools
from io import BytesIO

try:
    import http.client as client
    import urllib.parse as parse
    from urllib.request import urlopen as urlopen
    from urllib.request import Request as request
    from urllib.parse import urlencode as encode
    from configparser import ConfigParser   
# py2
except ImportError:
    import httplib as client
    from urllib2 import urlparse as parse
    from urllib2 import urlopen as urlopen
    from urllib2 import Request as request
    from urllib import urlencode as encode
    from ConfigParser import ConfigParser
    unicode = str

#TODO need to gracefully handle when these are not specified or if they have incorrect return values

starttime = None
tempFeatureCollectionItemID = None
gdbItemID = None
shh = None

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

def readConfig():
    
    config = ConfigParser()
    config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection_states.cfg')))

    global syncLOG
    syncLOG = validateInput(config, 'Log File Location', 'syncLog', 'path', True)

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

    global featureServiceItemID
    featureServiceItemID = validateInput(config, 'Existing ItemIDs', 'featureServiceItemID', 'id', True)

    global featureCollectionItemID
    featureCollectionItemID = validateInput(config, 'Existing ItemIDs', 'featureCollectionItemID', 'id', True)

def validateInput(config, group, name, type, required):
    #TODO if this just errors when the value is not supplied 
    value = config.get(group, name)
    if type == 'path':
        return os.path.normpath(value)
    elif type == 'mapping':
        if value.find(',') > -1:
            return list(v.split(',') for v in value.split(';'))
        else:
            print('Unable to parse name mapping')
    elif type == 'bool':
        return value.lower() == 'true'
    else:
        return value

def startLogging():
    # Logging Logic
        global starttime
        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        #TODO this should be handled as a part of validation logic...if it doens't exist create it
        fileNameSpecified = os.path.basename(syncLOG).find(".txt") > -1

        logfileLocation = os.path.abspath(os.path.dirname(syncLOG))
        if not os.path.exists(logfileLocation):
            os.makedirs(logfileLocation)

        #os.path.abspath(os.path.dirname(__file__)), "SyncLog"
        if fileNameSpecified:
            path = syncLOG
        else:
            path = os.path.join(syncLog, "SyncLog.txt")
        global logPath
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
        # Close out the log file
        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log = open(logPath,"a")
        log.write("     " + str(d) + " - " +myMessage + "\n")
        log.close()
        print("     " + str(d) + " - " +myMessage + "\n")

def endLogging():
    # Close out the log file
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
    print(pymsg)
    raise

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
    response_text = response_bytes.decode('UTF-8')
    #return json.loads(response_text)
    return response_text

def getPublishedItems():
    admin = manageorg.Administration(securityHandler=shh.securityhandler)
    content = admin.content

    #Test if item ID does not exist and return error message
    item = content.getItem(itemId=featureServiceItemID)
    if item.id is None:
        logMessage('Error: Unable to find feature service with ID: {}'.format(featureServiceItemID))
        sys.exit(1)

    global baseName
    baseName= item.title # item.name is returning None for feature service created from FGDB

    global SR
    SR = item.spatialReference

    #Get the prepublished feature collection name
    fcItem = content.getItem(itemId=featureCollectionItemID)
    if fcItem.id is None:
        logMessage('Error: Unable to find feature collection with ID: {}'.format(featureCollectionItemID))
        sys.exit(1)

def uploadFGDB():
    #ToDo need a better way to search for the item, below is searching the entire organization, which may return many results and not necessarly the GDB we are looking for.
    
    org = manageorg.Administration(securityHandler=shh.securityhandler)
    #result = org.search(baseName,bbox = None)
    #keyset = ['results']

    #value = None
    #for key in keyset:
    #    if key in result:
    #        value = result[key]
    #        if not (value == []):
    #            existingTitles = [d['title'] for d in value]
    #            existingIDs =[d['id'] for d in value]
    #            existingTypes =[d['type'] for d in value]
    #            existingItems = dict(zip(existingTypes, existingIDs))
    #            if "File Geodatabase" in existingItems:
    #                logMessage("File geodatabase {} found on the portal, deleting the item".format(baseName))
    #                deleteItem(existingItems['File Geodatabase'])
    #                logMessage("File geodatabase deleted")

    logMessage("Uploading file geodatabase")

    itemParams = arcrest.manageorg.ItemParameter()
    itemParams.title = baseName #this name should be derived from the fGDB
    itemParams.type = "File Geodatabase"
    itemParams.tags = "GDB"
    itemParams.typeKeywords = "Data,File Geodatabase"

    content = org.content
    usercontent = content.users.user(username)
    if isinstance(usercontent, arcrest.manageorg.administration._content.User):
        pass

    gdbSize = os.path.getsize(fgdb)

    #TODO add check for file size...if larger than 100 MBs we should set multipart to true
    #see ArcREST _content.addItem
    result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb)

    global gdbItemID
    gdbItemID = result.id

    logMessage("File geodatabase upload complete")

def updateFeatureService():
    logMessage("Updating {} feature service".format(baseName))

    org = manageorg.Administration(securityHandler=shh.securityhandler)
    content = org.content
    usercontent = content.users.user(username)

    fst = featureservicetools.featureservicetools(shh)
    fs = fst.GetFeatureService(itemId=featureServiceItemID,returnURLOnly=False)

    publishParams = json.loads(getJSON(fs.url))
    publishParams['name'] = baseName
    layers = json.loads(getJSON(fs.url + "/layers"))
    publishParams['layers'] = layers['layers']
    publishParams['tables'] = layers['tables']

    #publishParams = arcrest.manageorg.PublishFGDBParameter(name=baseName,
    #    layerInfo=None,
    #    description=None,
    #    maxRecordCount=None,
    #    copyrightText=None,
    #    targetSR=None)
   
    result = usercontent.publishItem(fileType="fileGeodatabase", 
                                        #publishParameters=publishParams,
                                        publishParameters=CustomPublishParameter(publishParams),  
                                        itemId=gdbItemID, 
                                        wait=True, overwrite=True)

    logMessage("{} feature service updated".format(baseName))

def exportTempFeatureCollection():
    logMessage("Exporting " + baseName + " to a temporary feature collection")
    FCtemp = baseName + "_temp"     

    org = manageorg.Administration(securityHandler=shh.securityhandler)
    content = org.content
    usercontent = content.users.user(username)
    expParams = {"maxAllowableOffset":maxAllowableOffset}
 
    result = usercontent.exportItem(title=FCtemp,
                                itemId=featureServiceItemID,
                                exportFormat="feature collection",
                                exportParameters=expParams,
                                wait=True)

    global tempFeatureCollectionItemID
    tempFeatureCollectionItemID = result.id        
    logMessage("Temp feature collection created")                                                           

def updateFeatureCollection():
    admin = manageorg.Administration(securityHandler=shh.securityhandler)
    content = admin.content
    item = content.getItem(featureCollectionItemID)

    logMessage("Updating {} feature collection".format(item.name))

    updatedFeatures = getJSON(orgURL + "/sharing/rest/content/items/" + tempFeatureCollectionItemID + "/data")
    
    d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    itemParams = manageorg.ItemParameter()
    itemParams.snippet = str(d)
    item.userItem.updateItem(itemParameters=itemParams, text=updatedFeatures)
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