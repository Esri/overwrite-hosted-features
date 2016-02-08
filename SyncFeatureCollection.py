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

#TODO need to gracefully handle when these are not specified or if they have incorrect return values

##Lessons Learned...if the FGDB ItemTitle does not match the base FGDB Name then publishing from the FGDB fails

fcType = "feature collection"
starttime = None

def validateInput(config, group, name, type, required):
    #TODO extend this to include extra validation that we can find IDs and whatnot
    try:
        #TODO if this just errors when the value is not supplied 
        value = config.get(group, name)
        if(type == 'path'):
            return os.path.normpath(value)
        else:
            return value
    except:
        if required:
            showError(sys.exc_info()[2])  
        else:
            if type == 'bool':
                return False
            else:
                return None

def setBaseName(gdbZip):
    n = os.path.splitext(os.path.basename(gdbZip))[0]
    if n.find('.gdb'):
        global baseName
        baseName = os.path.splitext(n)[0]
        global FCtemp 
        FCtemp = baseName + "_temp"

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
        log.write("Begin "+ baseName + " Data Sync:\n")
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
                logMessage(baseName + " File Geodatabase Zip file does not exist at " + os.path.dirname(os.path.repalpath(file_Name)))

def showError(tb):
    tbinfo = traceback.format_tb(tb)[0]
    pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
    logMessage("" + pymsg)
    print(pymsg)

def chunklist(l, n):
    n = max(1, n)
    for i in range(0, len(l), n):
        yield l[i:i+n]

def updateProductionFC(tempFC_ID):
    try:
        #Start Logging
        logMessage("Update " + FCtitle + " production feature collection")
        
        admin = manageorg.Administration(url=baseURL, securityHandler=sh)
        content = admin.content
        item = content.getItem(featureCollectionItemID)
        usercontent = content.users.user(username)

        #Get JSON file to be passed into the production Feature Collection.
        #with open(jsonExport, 'r', encoding='utf-8', errors='ignore') as layerDef:
        if vi == 2:
            with open(jsonExport, 'r') as layerDef:
                updatedFeatures = layerDef.readline()
        else:
            exportItem = content.getItem(itemId=tempFC_ID)
            updatedFeatures = exportItem.itemData(f="json")

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
                logMessage(FCtitle + " was successfully updated!")
                loggingEnd("Production feature collection was successully updated!")
                print("Sync complete, Feature Collection updated")
                exit
            else:
                exit
        else:
            exit
    except:
        showError(sys.exc_info()[2])

def export_tempFeatureCollection():
    try:
        #Start Logging
        logMessage("Exporting " + FCtemp + " temporary feature collection")

        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, manageorg.administration._content.User):
            pass

        expParams = None
        if enableQuantization:
            expParams = {"maxAllowableOffset":maxAllowableOffset,"quantizationParameters":{"tolerance":tolerance}}

        result = usercontent.exportItem(title=FCtemp,
                                    itemId=featureServiceItemID,
                                    exportFormat=fcType,
                                    exportParameters=expParams,
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
        #else:
        #    response = urllib.request.urlretrieve(url, jsonExport)

        #Temporary Feature Collection created and new data captured
        logMessage(FCtemp + " created... preparing to update production" + FCtitle + " feature collection.")
        updateProductionFC(exportedItemId)
    except:
        showError(sys.exc_info()[2])

def deleteFGDB(myContent):
    try:
        logMessage("Removing  " + baseName + " File Geodatabase.")

        admin = manageorg.Administration(url=baseURL, securityHandler=sh)
        gdb_itemId = myContent['File Geodatabase']
        item = admin.content.getItem(gdb_itemId)
        usercontent = content.users.user(username)
        result = usercontent.deleteItems(items=gdb_itemId)

        if 'error' in result:
            print(result)
        else:
            logMessage(baseName + " File Geodatabase removed.")
    except:
        showError(sys.exc_info()[2])

def clearTempContent():
    org = arcrest.manageorg.Administration(url=baseURL, securityHandler=sh)
    content = org.content
    usercontent = content.users.user(username)
    #TODO verify that this has a value
    #gdbItem = content.getItem(itemId=gdbID)
    #usercontent.deleteItems(items=gdbItem)

def publishTempFeatureService(gdbID, myContent, version):
    try:
        logMessage("Publishing temp " + baseName + "feature service")

        org = manageorg.Administration(url=baseURL, securityHandler=sh)

        publishParams = arcrest.manageorg.PublishFGDBParameter(name=baseName,
            layerInfo=None,
            description=description,
            maxRecordCount=-1,
            copyrightText=licenseInfo,
            targetSR=SR)

        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, manageorg.administration._content.User):
            pass
        result = usercontent.publishItem(fileType="fileGeodatabase", 
                                         publishParameters=publishParams, 
                                         itemId=gdbID, 
                                         wait=True)
        print(result.url)
        updateProductionFS(result.url)
        usercontent.deleteItems(items=result.id)

        logMessage(baseName + " feature service publishing complete... preparing to export to a feature collection.")

        export_tempFeatureCollection()
    except:
        showError(sys.exc_info()[2])

def updateProductionFS(url):
    logMessage("Preparing to update " + productionFSName)
    productionFS = arcrest.agol.services.FeatureService(url=productionURL,
        securityHandler=sh,
        proxy_port=None,
        proxy_url=None,
        initialize=True)

    for lyr in productionFS.layers:
        #TODO need to handle name map here 
        lyrUrl = url + "/" + str(lyr.id)
        updatedFL = arcrest.agol.services.FeatureLayer(url=lyrUrl,
            securityHandler=sh,
            proxy_port=None,
            proxy_url=None,
            initialize=True)
        lyr.deleteFeatures(where="1=1")

        result = updatedFL.query(where='1=1', returnIDsOnly=True)
        print(result)
        if 'error' in result:
            print(result)
            return result
        else:
            chunksize = min(lyr.maxRecordCount, updatedFL.maxRecordCount)
            oids = result['objectIds']
            total = len(oids)
            if len(oids) > chunksize:
                print ("%s features to be downloaded" % total)
                totalQueried = 0
                for chunk in chunklist(l=oids, n=chunksize):
                    oidsQuery = ",".join(map(str, chunk))
                    if not oidsQuery:
                        continue
                    else:
                        results = updatedFL.query(objectIds=oidsQuery,
                                            returnGeometry=True,
                                            out_fields="*")
                        if isinstance(results, arcrest.common.general.FeatureSet):
                            lyr.applyEdits(addFeatures=results.features)
                            totalQueried += len(results.features)
                            print("{:.0%} Completed: {}/{}".format(totalQueried / float(total), totalQueried, total))
                        else:
                            print (results)
            else:
                results = updatedFL.query(objectIds=oidsQuery,
                                          returnGeometry=True,
                                          out_fields="*")
                lyr.applyEdits(addFeatures=results.features)
    logMessage(productionFSName + " updated sucessfully!")

def uploadFGDB():
    try:
        logMessage("Uploading " + baseName + " File Geodatabase")

        itemParams = arcrest.manageorg.ItemParameter()
        itemParams.title = baseName #this name should be derived from the fGDB
        itemParams.type = "File Geodatabase"
        itemParams.tags = tags
        itemParams.typeKeywords = "Data,File Geodatabase"

        org = arcrest.manageorg.Administration(url=baseURL, securityHandler=sh)
        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, arcrest.manageorg.administration._content.User):
            pass

        gdbSize = os.path.getsize(fgdb1)

        #TODO add check for file size...if larger than 100 MBs we should set multipart to true
        #see ArcREST _content.addItem
        result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb1)

        global gdbID
        gdbID = result.id

        logMessage(baseName + " File Geodatabase upload completed... preparing to publish as a feature service.")
        publishTempFeatureService(result.id, None, None)

        usercontent.deleteItems(items=result.id)

    except:
        showError(sys.exc_info()[2])

def sync_Init():
    try:
        loggingStart("Syncronize " + baseName + " Web Application with updated data")
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        result = org.search(baseName,bbox = None)
        keyset = ['results']

        value = None
        for key in keyset:
            if key in result:
                value = result[key]
                if (value == []):
                    print("The query for " + baseName.replace(" ","") + " came up with no results")
                    uploadFGDB()
                else:
                    existingTitles = [d['title'] for d in value]
                    existingIDs =[d['id'] for d in value]
                    existingTypes =[d['type'] for d in value]
                    existingItems = dict(zip(existingTypes, existingIDs))
                    if "File Geodatabase" in existingItems:
                        deleteFGDB(existingItems)
                        uploadFGDB()
                    else:
                        print("The query for " + baseName.replace(" ","") + " came up with no File Geodatabase results")
                        uploadFGDB()

    except:
        showError(sys.exc_info()[2])

def getPrePublishedInfo():
    admin = manageorg.Administration(url=baseURL, securityHandler=sh)
    content = admin.content

    #TODO test when this item ID does not exist or returns an unexpected type and handle gracefully
    item = content.getItem(itemId=featureServiceItemID)
    if 'error' in item:
        print(item)
        exit

    #GET details from the prepublished feature service
    global productionURL
    productionURL = item.url

    global productionFSName
    productionFSName = item.name

    global SR
    SR = item.spatialReference

    #TODO only do this on an inital publish...otherwise if the user modified the values on the production FC they would be overwitten by what'sin the Feature Service
    global thumbnail
    thumbnail = item.thumbnail
    _tags = []
    for tag in item.tags:
        _tags.append(str(tag))
    global tags
    tags = _tags
    global snippet
    snippet = str(item.snippet)
    global description
    description = str(item.description)
    global licenseInfo
    licenseInfo = str(item.licenseInfo)

    #GET the prepublished feature collection name
    fcItem = content.getItem(itemId=featureCollectionItemID)
    if 'error' in fcItem:
        print(fcItem)
        exit

    global FCtitle
    FCtitle = fcItem.name

def readConfig():
    
    config = ConfigParser()

    try:
        config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection.cfg')))
    except:
        logMessage("Error loading configuration file: " + os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection.cfg'))
        showError(sys.exc_info()[2]) 
    global syncLOG
    syncLOG = validateInput(config, 'Log File Location', 'syncLog', 'path', True)

    global fgdb1
    fgdb1 = validateInput(config, 'Data Sources', 'fgdb', 'path', True)

    global jsonExport
    jsonExport = validateInput(config, 'JSON Export', 'jsonExport', 'path', True)

    global baseURL
    baseURL = validateInput(config, 'Portal Sharing URL', 'baseURL', 'url', True) + "sharing/rest"

    global username
    username = validateInput(config, 'Portal Credentials', 'username', 'string', True)

    global pw
    pw = validateInput(config, 'Portal Credentials', 'pw', 'string', True)

    global enableQuantization
    enableQuantization = validateInput(config, 'Quantization', 'enableQuantization', 'bool', False)

    global maxAllowableOffset
    maxAllowableOffset = validateInput(config, 'Quantization', 'maxAllowableOffset', 'int', False)

    global tolerance
    tolerance = validateInput(config, 'Quantization', 'tolerance', 'int', False)

    global featureServiceItemID
    featureServiceItemID = validateInput(config, 'Existing ItemIDs', 'featureServiceItemID', 'id', True)

    global featureCollectionItemID
    featureCollectionItemID = validateInput(config, 'Existing ItemIDs', 'featureCollectionItemID', 'id', True)

    global nameMapping
    nameMapping = validateInput(config, 'Layer Name Mapping', 'nameMapping', 'mapping', False)

def main():
    readConfig()

    global sh
    sh = arcrest.AGOLTokenSecurityHandler(username=username, password=pw)
    
    getPrePublishedInfo()
    setBaseName(fgdb1)
    sync_Init()

if __name__ == "__main__":
    try:
        main()
    except:
        print("should check the temp IDs here and cleanup anything that's left over")
    finally:
        clearTempContent()