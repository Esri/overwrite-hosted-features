import datetime, os, sys, time, traceback, urllib
import arcrest, arcresthelper
from arcrest import manageorg
from arcresthelper import featureservicetools

vi = sys.version_info[0]
if vi == 2:
    from ConfigParser import ConfigParser
else: 
    from configparser import ConfigParser

#TODO need to gracefully handle when these are not specified or if they have incorrect return values

starttime = None
tempFeatureCollectionItemID = None
gdbItemID = None
shh = None
orgURL = None

def readConfig():
    
    config = ConfigParser()

    try:
        config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection_single.cfg')))
    except:
        logError(sys.exc_info()[2]) 
    global syncLOG
    syncLOG = validateInput(config, 'Log File Location', 'syncLog', 'path', True)

    global fgdb
    fgdb = validateInput(config, 'Data Sources', 'fgdb', 'path', True)

    global jsonExport
    jsonExport = validateInput(config, 'JSON Export', 'jsonExport', 'path', True)

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
    #TODO extend this to include extra validation that we can find IDs and whatnot
    try:
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
    except:
        if required:
            logError(sys.exc_info()[2])  
        else:
            if type == 'bool':
                return False
            else:
                return None

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

def chunklist(l, n):
    n = max(1, n)
    for i in range(0, len(l), n):
        yield l[i:i+n]

def getPublishedItems():
    admin = manageorg.Administration(securityHandler=shh.securityhandler)
    content = admin.content

    #Test if item ID does not exist and return error message
    item = content.getItem(itemId=featureServiceItemID)
    if item.id is None:
        logMessage('Error: Unable to find feature service with ID: {}'.format(featureServiceItemID))
        sys.exit(1)

    global baseName
    baseName= item.name

    global SR
    SR = item.spatialReference

    #GET the prepublished feature collection name
    fcItem = content.getItem(itemId=featureCollectionItemID)
    if fcItem.id is None:
        logMessage('Error: Unable to find feature collection with ID: {}'.format(featureCollectionItemID))
        sys.exit(1)

def uploadFGDB():
    try:
        org = manageorg.Administration(securityHandler=shh.securityhandler)
        result = org.search(baseName,bbox = None)
        keyset = ['results']

        value = None
        for key in keyset:
            if key in result:
                value = result[key]
                if not (value == []):
                    existingTitles = [d['title'] for d in value]
                    existingIDs =[d['id'] for d in value]
                    existingTypes =[d['type'] for d in value]
                    existingItems = dict(zip(existingTypes, existingIDs))
                    if "File Geodatabase" in existingItems:
                        deleteFGDB(existingItems)


        logMessage("Uploading file geodatabase")

        itemParams = arcrest.manageorg.ItemParameter()
        itemParams.title = baseName #this name should be derived from the fGDB
        itemParams.type = "File Geodatabase"
        itemParams.tags = "GDB"
        itemParams.typeKeywords = "Data,File Geodatabase"

        org = arcrest.manageorg.Administration(securityHandler=shh.securityhandler)
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
    except:
        logError(sys.exc_info()[2])

def deleteFGDB(myContent):
    try:
        logMessage("File geodatabase {} found on the portal, deleting the item".format(baseName))

        org = manageorg.Administration(securityHandler=shh.securityhandler)
        gdb_itemId = myContent['File Geodatabase']
        item = org.content.getItem(gdb_itemId)
        usercontent = org.content.users.user(username)
        result = usercontent.deleteItems(items=gdb_itemId)
        logMessage("File geodatabase deleted")
    except:
        logError(sys.exc_info()[2])

def updateFeatureService():
    try:
        logMessage("Updating {} feature service".format(baseName))

        org = manageorg.Administration(securityHandler=shh.securityhandler)

        publishParams = arcrest.manageorg.PublishFGDBParameter(name=baseName,
            layerInfo=None,
            description=None,
            maxRecordCount=None,
            copyrightText=None,
            targetSR=None)

        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, manageorg.administration._content.User):
            pass
        result = usercontent.publishItem(fileType="fileGeodatabase", 
                                         publishParameters=publishParams, 
                                         itemId=gdbItemID, 
                                         wait=True, overwrite=True)

        logMessage("{} feature service updated".format(baseName))
    except:
        logError(sys.exc_info()[2])

def exportTempFeatureCollection():
    try:
        logMessage("Exporting " + baseName + " to a temporary feature collection")
        FCtemp = baseName + "_temp"     

        org = manageorg.Administration(securityHandler=shh.securityhandler)
        content = org.content
        usercontent = content.users.user(username)

        fst = featureservicetools.featureservicetools(shh)
        fs = fst.GetFeatureService(itemId=featureServiceItemID,returnURLOnly=False)
      
        expLayers = []
        for lyr in fs.layers:
            layerDict = {'id' : lyr.id}
            layerDict.update({"maxAllowableOffset":maxAllowableOffset})
            expLayers.append(layerDict)
        expParams = {'layers': expLayers}
 
        result = usercontent.exportItem(title=FCtemp,
                                    itemId=featureServiceItemID,
                                    exportFormat="feature collection",
                                    exportParameters=expParams,
                                    wait=True)

        global tempFeatureCollectionItemID
        tempFeatureCollectionItemID = result.id
        ##TODO see if I can get the itemData as JSON like below
        exportItem = content.getItem(itemId=tempFeatureCollectionItemID)

        #Export Temporary Feature Collection as in memory JSON response
        #jsonExport = exportItem.itemData(f="json")
        token = shh.securityhandler.token
        url = orgURL + "/content/items/" + tempFeatureCollectionItemID + "/data?token=" + token + "&f=json"
        
        if vi == 2:
            response = urllib.urlretrieve(url, jsonExport)
        #else:
        #    response = urllib.request.urlretrieve(url, jsonExport)

        #Temporary Feature Collection created and new data captured
        logMessage("Temp feature collection created")
    except:
        logError(sys.exc_info()[2])

def updateProductionFC():
    try:
        admin = manageorg.Administration(securityHandler=shh.securityhandler)
        content = admin.content
        item = content.getItem(featureCollectionItemID)
        usercontent = content.users.user(username)

        logMessage("Updating {} feature collection".format(item.name))

        #Get JSON file to be passed into the production Feature Collection.
        #with open(jsonExport, 'r', encoding='utf-8', errors='ignore') as layerDef:
        if vi == 2:
            with open(jsonExport, 'r') as layerDef:
                updatedFeatures = layerDef.readline()
        else:
            exportItem = content.getItem(itemId=tempFeatureCollectionItemID)
            updatedFeatures = exportItem.itemData(f="json")

        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        item.userItem.updateItem(itemParameters=manageorg.ItemParameter(), text=updatedFeatures)
        logMessage("{} feature collection updated".format(item.name))
    except:
        logError(sys.exc_info()[2])

def removeTempContent():
    #Remove any temp items that were not cleaned up due to failures in the script
    if shh is not None:
        org = manageorg.Administration(securityHandler=shh.securityhandler)
        content = org.content
        usercontent = content.users.user(username)
        
        if gdbItemID is not None:
            gdbItem = content.getItem(itemId=gdbItemID)
            if gdbItem.id is not None:
                usercontent.deleteItems(items=gdbItem.id)
                logMessage("File geodatabase deleted")

        if tempFeatureCollectionItemID is not None:
            fcItem = content.getItem(itemId=tempFeatureCollectionItemID)
            if fcItem.id is not None:
                usercontent.deleteItems(items=fcItem.id)
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
    updateProductionFC()
    
if __name__ == "__main__":
    try:
        main()
    finally:
        removeTempContent()
        endLogging()  