import datetime, json, os, sys, subprocess, time, traceback, uuid, urllib, zipfile, json
from subprocess import Popen
from arcrest import manageorg
import arcrest
from arcrest.agol import FeatureService
from collections import OrderedDict 

vi = sys.version_info[0]
if vi == 2:
    from ConfigParser import ConfigParser
else: 
    from configparser import ConfigParser

## Migrated to ArcREST 3.5.1

##Lessons Learned...if the FGDB ItemTitle does not match the base FGDB Name then publishing from the FGDB fails

config = ConfigParser()
config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollectionModified.cfg')))
syncSchedule = os.path.normpath(config.get('Log File Location', 'syncSchedule'))
syncLOG = os.path.normpath(config.get('Log File Location', 'syncLog'))
fgdb1 = os.path.normpath(config.get('Data Sources', 'fgdb1'))
jsonExport = os.path.normpath(config.get('JSON Export', 'jsonExport'))
baseURL = config.get('Portal Sharing URL', 'baseURL') + "sharing/rest"
username = config.get('Portal Credentials', 'username')
pw = config.get('Portal Credentials', 'pw')
updateInterval = int(config.get('Update Interval', 'updateInterval'))* 60

##TODO if possible it would be nice if we could just take in the SD file from disk
## if we can in some way know what the name should be and find it after the inital add
itemID = os.path.normpath(config.get('Existing ItemID', 'itemID'))

gdbType = "File Geodatabase"
fcType = "feature collection"
starttime = None

#TODO
# should pull the SR from the prepublished item

def setAppName(gdbZip):
    n = os.path.splitext(os.path.basename(gdbZip))[0]
    if n.find('.gdb'):
        global appProgram
        appProgram = os.path.splitext(n)[0]
        global FStitle
        FStitle = appProgram
        global FCtitle
        FCtitle = appProgram
        global FCtemp 
        FCtemp = appProgram + "_temp"

def trace():
    """
        trace finds the line, the filename
        and error message and returns it
        to the user
    """
    import traceback
    import sys
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # script name + line number
    line = tbinfo.split(", ")[1]
    # Get Python syntax error
    #
    synerror = traceback.format_exc().splitlines()[-1]
    return line, __file__, synerror

def loggingStart(currentProcess):
    # Logging Logic
        global starttime
        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log = open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "SyncLog", "SyncLog.txt"),"a")
        log.write("----------------------------" + "\n")
        log.write("----------------------------" + "\n")
        log.write("Log: " + str(d) + "\n")
        log.write("\n")
        # Start process...
        starttime = datetime.datetime.now()
        log.write("Begin "+ appProgram + " Data Sync:\n")
        log.write("     " + str(starttime.strftime('%Y-%m-%d %H:%M:%S')) +" - " + currentProcess + "\n")
        log.close()

def logMessage(myMessage):
        # Close out the log file
        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log = open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "SyncLog", "SyncLog.txt"),"a")
        log.write("     " + str(d) + " - " +myMessage + "\n")
        log.close()
        print("     " + str(d) + " - " +myMessage + "\n")

def loggingEnd(endingProcess):
        # Close out the log file
        global starttime
        log = open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "SyncLog", "SyncLog.txt"),"a")
        endtime = datetime.datetime.now()
         # Process Completed...
        log.write("     " + str(endtime.strftime('%Y-%m-%d %H:%M:%S')) + " - " + endingProcess + " completed successfully!"
           + "\n" + "Elapsed time " + str(endtime - starttime) + "\n")
        log.write("\n")
        log.close()

def watchDog(file_Name, attempts=0, timeout=5, sleep_int=5, total_Attempts=5):

    if attempts < timeout and os.path.exists(file_Name) and os.path.isfile(file_Name):
        try:
            results = os.path.exists(file_Name)
            if results == True:
                return results
        except:
            # perform an action
            if (attempts + 1 <= total_Attempts):
                sleep(sleep_int)
                watchDog(file_Name, attempts + 1)
            else:
                logMessage(FStitle + " File Geodatabase Zip file does not exist at " + os.path.dirname(os.path.repalpath(file_Name)))

def timer():
    while True:
        time.sleep(updateInterval)
        sync_Init()
        #Popen([os.path.join(os.path.abspath(sys.exec_prefix),'python.exe'), os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection.py')])
        #sys.exit(0)

def showError(tb):
    tbinfo = traceback.format_tb(tb)[0]

    # Concatenate information together concerning
    # the error into a message string
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    # Python / Python Window
    logMessage("" + pymsg)
    print(pymsg)

def updateProductionFC(productionDict, tempFC_ID):
    try:
        #Start Logging
        logMessage("Update " + FCtitle + " production feature collection")

        #Update Logic Begins here...
        sh = arcrest.AGOLTokenSecurityHandler(username=username, password=pw)
        admin = manageorg.Administration(url=baseURL, securityHandler=sh)
        gdb_itemId = productionDict['Feature Collection']
        content = admin.content
        item = content.getItem(gdb_itemId)
        usercontent = content.users.user(username)

        #Get JSON file to be passed into the production Feature Collection.
        #with open(jsonExport, 'r', encoding='utf-8', errors='ignore') as layerDef:
        with open(jsonExport, 'r') as layerDef:
            updatedFeatures = layerDef.readline()

        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Set the itemParameters
        itemParams = manageorg.ItemParameter()
        itemParams.title = FCtitle
        itemParams.tags = tags
        itemParams.thumbnail = thumbnail
        itemParams.description = description
        itemParams.snippet = snippet + " as of " + str(d)
        itemParams.licenseInfo = licenseInfo
        itemParams.overwrite = True

        print("Updating production Feature Collection")
        result = item.userItem.updateItem(itemParameters=itemParams, text=updatedFeatures)

        if result['success'] is True:
            #delete temporary Feature Content
            temporaryItem = content.getItem(itemId=tempFC_ID)
            delResults = usercontent.deleteItems(items=temporaryItem.id)

            response = delResults['results'].pop(-1)
            status = response['success']
            if status is True:
                if (os.path.exists(jsonExport)):
                   os.unlink(jsonExport)
                # Implement at final... removes old FGDB Zip file.
                #os.remove(fGDB)
                logMessage(FCtitle + " was successfully updated!")
                loggingEnd("Production feature collection was successully updated!")
                print("Sync complete, Feature Collection updated")
                timer()
            else:
                exit
        else:
            exit
    except:
        showError(sys.exc_info()[2])

def export_tempFeatureCollection(myContentDict):
    try:
        #Start Logging
        logMessage("Exporting " + FCtemp + " temporary feature collection")

        # Publish FS Logic
        # Create security handler
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        content = org.content
        usercontent = content.users.user(username)
        #Get the Updated Feature Service item id
        fsID = myContentDict['Feature Service']
        if isinstance(usercontent, manageorg.administration._content.User):
            pass

        result = usercontent.exportItem(title=FCtemp,
                                    itemId=fsID,
                                    exportFormat=fcType,
                                    exportParameters=None,
                                    wait=True)

        exportedItemId = result.id
        ##TODO see if I can get the itemData as JSON like below
        exportItem = content.getItem(itemId=exportedItemId)

        #Export Temporary Feature Collection as in memory JSON response
        #jsonExport = exportItem.itemData(f="json")
        token = sh.token
        url = baseURL + "/content/items/" + exportedItemId + "/data?token=" + token + "&f=json"
        
        if vi == 2:
            response = urllib.urlretrieve(url, jsonExport)
        else:
            response = urllib.request.urlretrieve(url, jsonExport)
        #Temporary Feature Collection created and new data captured
        logMessage(FCtemp + " created... preparing to update production" + FCtitle + " feature collection.")
        updateProductionFC(myContentDict, exportedItemId)
    except:
        showError(sys.exc_info()[2])

def updateFS(contentDict,gdb_Source):
    try:
        logMessage("Updating " + FStitle + " Feature Service with data from " + FStitle + " File Geodatabase")

        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        content = org.content

        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        updatedDescrpt = "Updated " + str(d) + "\n GDB Version " + str(gdb_Source)

        gdbID = contentDict['File Geodatabase']

        if 'Feature Service' in contentDict:
            fsID = contentDict['Feature Service']
            item = content.getItem(fsID)
            itemParams = manageorg.ItemParameter()
            itemParams.title=FStitle
            itemParams.description= updatedDescrpt
            print("Update Feature Service...")
            item.userItem.updateItem(itemParameters=itemParams, data=fgdb1)
            updateDefinitions(item.url)
        else:
            publishParams = arcrest.manageorg.PublishFGDBParameter(
            name=FStitle,
            layerInfo=None,
            maxRecordCount=-1,
            copyrightText=licenseInfo,
            targetSR=102100)
            usercontent = content.users.user(username)
            if isinstance(usercontent, manageorg.administration._content.User):
                pass
            print("Publish Feature Service...")
            result = usercontent.publishItem(itemId=gdbID, fileType="fileGeodatabase", publishParameters=publishParams, wait=True)
            updateDefinitions(result.url)

        logMessage(FStitle + " feature service publishing complete... preparing to export to a feature collection.")
        export_tempFeatureCollection(contentDict)

    except:
        showError(sys.exc_info()[2])

def updateFGDB(myContent):
    try:
        #Log startup message
        logMessage("Updating " + FStitle + " File Geodatabase item with new data")
        version = None

        fGDB = fgdb1

        # Check to make sure the updated FileGDB zipfile exists.
        results = watchDog(fGDB)
        if results == True:
            logMessage("Located " + FStitle + " file geodatabase, continuing the update.")

        # Update Logic Begins here...
        sh = arcrest.AGOLTokenSecurityHandler(username=username, password=pw)
        admin = manageorg.Administration(url=baseURL, securityHandler=sh)
        gdb_itemId = myContent['File Geodatabase']
        content = admin.content
        item = content.getItem(gdb_itemId)
        usercontent = content.users.user(username)

        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        itemParams = manageorg.ItemParameter()
        itemParams.title=FStitle
        itemParams.description="Updated " + str(d) + "\n GDB Version " + str(version)

        status = item.userItem.updateItem(itemParameters=itemParams, data=fGDB)
        print("Updating File Geodatabase")

        logMessage(FStitle + "File Geodatabase Updated!")
        logMessage("Preparing to update " + FStitle + " Feature Service")
        updateFS(myContent, version)


    except:
        showError(sys.exc_info()[2])

def exportFeatureCollection(fsID):
    try:
        #Start Logging
        logMessage("Exporting " + FStitle + " feature service to a feature collection")
        print("Exporting " + FStitle + " feature service to a feature collection")
        # Publish FS Logic
        # Create security handler
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = manageorg.Administration(url=baseURL, securityHandler=sh)

        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, manageorg.administration._content.User):
            pass
        result = usercontent.exportItem(title=FCtitle,
                                            itemId=fsID,
                                            exportFormat=fcType,
                                            exportParameters=None,
                                            wait=True)

        # Set the itemParameters
        itemParams = manageorg.ItemParameter()
        itemParams.title = FCtitle
        itemParams.tags = tags
        itemParams.thumbnail = thumbnail
        itemParams.description = description
        itemParams.snippet = snippet
        itemParams.licenseInfo = licenseInfo

        productionFC = result.updateItem(itemParameters=itemParams)

        if 'success' in productionFC:
            logMessage(FCtitle + " feature collection created.")
            loggingEnd("Feature Collection Data Upload Complete!")
            timer()
        else:
            exit
    except:
        showError(sys.exc_info()[2])

def publishFeatureService(gdbID):
    try:
        logMessage("Publishing " + FStitle + "feature service")
        print("Publishing " + FStitle + "feature service")
        # Publish FS Logic
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        print(description)
        publishParams = arcrest.manageorg.PublishFGDBParameter(name=FStitle,
            layerInfo=None,
            description=description,
            maxRecordCount=-1,
            copyrightText=licenseInfo,
            targetSR=102100)
        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, manageorg.administration._content.User):
            pass
        result = usercontent.publishItem(fileType="fileGeodatabase", publishParameters=publishParams, itemId=gdbID, wait=True)
        updateDefinitions(result.url)
        logMessage(FStitle + " feature service publishing complete... preparing to export to a feature collection.")
        exportFeatureCollection(result.item.id)

    except:
        showError(sys.exc_info()[2])

def updateDefinitions(url):
    sh = arcrest.AGOLTokenSecurityHandler(username, pw)
    org = manageorg.Administration(url=baseURL, securityHandler=sh)
    fs = arcrest.agol.services.FeatureService(url=url,
        securityHandler=sh,
        proxy_port=None,
        proxy_url=None,
        initialize=True)

    idx = url.find("rest/services")
    if idx > -1:
        idx += 5
        url = url[:idx] + 'admin/' + url[idx:]
    
    for lyr in fs.layers:
        lyrUrl = url + "/" + str(lyr.id)
        d = arcrest.hostedservice.AdminFeatureServiceLayer(lyrUrl, securityHandler=sh, initialize=True)
        sfs = d.updateDefinition(drawingInfos[lyr.id])

def uploadFGDB():
    try:
        # Log Step
        logMessage("Uploading " + FStitle + " File Geodatabase")
        print("Uploading " + FStitle + " File Geodatabase")
        # Sync Logic
        # Create security handler


        # Set the itemParameters
        itemParams = arcrest.manageorg.ItemParameter()
        itemParams.title = FStitle #this name should be derived from the fGDB
        itemParams.type = gdbType
        itemParams.tags = tags
        itemParams.typeKeywords = "Data,File Geodatabase"
        #itemParams.overwrite = "false",

        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = arcrest.manageorg.Administration(url=baseURL, securityHandler=sh)

        # Grab the user's content (items)
        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, arcrest.manageorg.administration._content.User):
            pass

        gdbSize = os.path.getsize(fgdb1)

        #TODO add check for file size...if larger than 100 MBs we should set multipart to true
        #see ArcREST _content.addItem
        result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb1)

        fgdb_itemID = result.id
        logMessage(FStitle + " File Geodatabase upload completed... preparing to publish as a feature service.")
        publishFeatureService(fgdb_itemID)

    except:
        showError(sys.exc_info()[2])

def sync_Init():
    try:
        # Start Logging
        loggingStart("Syncronize " + appProgram + " Web Application with updated data")
        # Check to see if File Geodatabase, Feature Service and Feature
        # Collection have been previously uploaded.
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        result = org.search("RFC5112",bbox = None)
        keyset = ['results']

        value = None
        for key in keyset:
            if key in result:
                value = result[key]
                if (value == []):
                    print("The query for " + appProgram.replace(" ","") + " came up with no results")
                    #Launch Uploader routine...
                    uploadFGDB()
                else:
                    existingTitles = [d['title'] for d in value]
                    existingIDs =[d['id'] for d in value]
                    existingTypes =[d['type'] for d in value]
                    dictionary = dict(zip(existingTypes, existingIDs))
                    #Launch Update routine...
                    if "Feature Collection" in dictionary:
                        updateFGDB(dictionary)
                    else:
                        print("The query for " + appProgram.replace(" ","") + " came up with no Feature Collection results")
                        #Launch Uploader routine...
                        uploadFGDB()

    except:
        showError(sys.exc_info()[2])

def getUserContent():
    content = org.content
    usercontent = content.users.user(username)

def getPrePublishedInfo():
    sh = arcrest.AGOLTokenSecurityHandler(username=username, password=pw)
    admin = manageorg.Administration(url=baseURL, securityHandler=sh)
    content = admin.content
    item = content.getItem(itemId=itemID)
    global thumbnail
    thumbnail = item.thumbnail
    global tags
    tags = item.tags
    global snippet
    snippet = item.snippet
    global description
    description = item.description
    global licenseInfo
    licenseInfo = item.licenseInfo

    fs = arcrest.agol.services.FeatureService(url=item.url,
        securityHandler=sh,
        proxy_port=None,
        proxy_url=None,
        initialize=True)

    _drawingInfos = {}

    for lyr in fs.layers:
        _drawingInfos[lyr.id] = {"drawingInfo" : lyr.drawingInfo}

    global drawingInfos
    drawingInfos = _drawingInfos

def main():
    getPrePublishedInfo()
    setAppName(fgdb1)
    sync_Init()

if __name__ == "__main__":
    main()