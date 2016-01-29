import datetime, json, os, sys, subprocess, time, traceback, uuid, urllib, zipfile
from configparser import ConfigParser
from subprocess import Popen
from arcrest import manageorg
import arcrest

## Migrated to ArcREST 3.5.1

##Lessons Learned...if the FGDB ItemTitle does not match the base FGDB Name then publishing from the FGDB fails

config = ConfigParser()
config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollectionModified.cfg')))
appProgram = config.get("Application Program Title", "appProgram")
syncSchedule = os.path.normpath(config.get('Log File Location', 'syncSchedule'))
syncLOG = os.path.normpath(config.get('Log File Location', 'syncLog'))
fgdb1 = os.path.normpath(config.get('Data Sources', 'fgdb1'))
jsonExport = os.path.normpath(config.get('JSON Export', 'jsonExport'))
baseURL = config.get('Portal Sharing URL', 'baseURL') + "sharing/rest"
username = config.get('Portal Credentials', 'username')
pw = config.get('Portal Credentials', 'pw')
FStitle = config.get('Item Title', 'FStitle')
FCtitle = config.get('Item Title', 'FCtitle')
FCtemp = config.get('Item Title', 'FCtemp')
tags = config.get('Service Description Info', 'tags')
snippet = config.get('Service Description Info', 'snippet')
description = config.get('Service Description Info', 'description')
licenseInfo = config.get('Service Description Info', 'licenseInfo')
thumbnail = os.path.normpath(config.get('Service Description Info', 'thumbnail'))
updateInterval = int(config.get('Update Interval', 'updateInterval'))* 60
#######################################
##TESTING
gdb_path = os.path.normpath(config.get('TEMP GDB', 'gdbPath'))
fc_name = os.path.normpath(config.get('FC NAME', 'fcName'))
constraining_fc = os.path.normpath(config.get('CONSTRAINING CLASS', 'constrainingFC'))
sd = json.loads(os.path.normpath(config.get('SD', 'sd')))
lllyyrrss = json.loads(os.path.normpath(config.get('LAYERS', 'l')))
num_pts = list(map(int, list(os.path.normpath(config.get('NP', 'np')).split(","))))
li = json.loads(os.path.normpath(config.get('LAYER INFO', 'li')))

syncGDB = None
gdbType = "File Geodatabase"
fcType = "feature collection"
starttime = None

#TODO
# should pull the SR from the prepublished item


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
        with open(jsonExport, 'r', encoding='utf-8', errors='ignore') as layerDef:
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
            temporaryItem = content.item(itemId=tempFC_ID)
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
        # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
            print(pymsg)

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
                                    exportParameters=li,
                                    wait=True)

        exportedItemId = result.id
        exportItem = content.getItem(itemId=exportedItemId)

        #Export Temporary Feature Collection as in memory JSON response
        #jsonExport = exportItem.itemData(f="json")
        token = sh.token
        url = baseURL + "/content/items/" + exportedItemId + "/data?token=" + token + "&f=json"
        response = urllib.request.urlretrieve(url, jsonExport)
        #Temporary Feature Collection created and new data captured
        logMessage(FCtemp + " created... preparing to update production" + FCtitle + " feature collection.")
        updateProductionFC(myContentDict, exportedItemId)
    except:
            # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
            print(pymsg)

def updateFS(contentDict,gdb_Source):
    try:
        logMessage("Updating " + FStitle + " Feature Service with data from " + FStitle + " File Geodatabase")
        # Publish FS Logic
        # Create security handler
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        updatedDescrpt = "Updated " + str(d) + "\n GDB Version " + str(gdb_Source)
        publishParams = arcrest.manageorg.PublishFGDBParameter(
                name=FStitle,
                layerInfo=sd,
                maxRecordCount=-1,
                copyrightText=licenseInfo,
                targetSR=102100)

        gdbID = contentDict['File Geodatabase']
        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, manageorg.administration._content.User):
            pass
        fsID = contentDict['Feature Service']
        deleted = 0
        if fsID not in [None, "", " ", []]:
            adminContent = org.content
            item = adminContent.getItem(fsID)
            result = item.userItem.deleteItem()
            deleted = 1
            
            ##TODO here is where we need to switch to updateItem...
            #item.userItem.updateItem(itemParameters=ip, data=fgdb1)

        print("Update Feature Service...")
        result = usercontent.publishItem(itemId=gdbID, fileType="fileGeodatabase", publishParameters=publishParams, wait=True)

        if deleted == 1:
            contentDict['Feature Service'] = result.item.id

        logMessage(FStitle + " feature service publishing complete... preparing to export to a feature collection.")
        export_tempFeatureCollection(contentDict)

    except:
        # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
            print(pymsg)

def updateFGDB(myContent):
    try:
        #Log startup message
        logMessage("Updating " + FStitle + " File Geodatabase item with new data")
        version = None
        #Check to see which FGDB updated last. Final script will look for a folder
##-------------------------------------------------------------------------------
        fGDB = fgdb1
        #This block is for testing only... switching back and forth between two sample Geodatabases
        #try:
        #    with open(syncSchedule, 'r') as GDBlog:
        #        try:
        #           lastGDBused = int(GDBlog.readline())
        #           if lastGDBused == 1:
        #                fGDB = fgdb2
        #                version = 2
        #           elif lastGDBused == 2:
        #                fGDB = fgdb1
        #                version = 1
        #        except:
        #            lastGDBused = 0

        #except IOError:
        #        with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "SyncSchedule.txt"),"w") as GDBlog:
        #            GDBlog.write(str(1))
##-------------------------------------------------------------------------------
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

        #usercontent.updateItem(itemId=gdb_itemId, updateItemParameters=itemParams, filePath=fGDB, async=True, overwrite=True)
        status = item.userItem.updateItem(itemParameters=itemParams, data=fGDB)
        #status =  usercontent.status(itemId=gdb_itemId, jobType="update")
        print("Updating File Geodatabase")
        #print(status)
        #while status['status'].lower() == "processing":
        #    time.sleep(3)
        #    print(status)
        #    status =  usercontent.status(itemId=gdb_itemId,
        #                                jobType="update")

        logMessage(FStitle + "File Geodatabase Updated!")

##-------------------------------------------------------------------------------
        # Temporary code for Testing To Be Removed at Final
        # Update the sync schedule file
        #if lastGDBused == 1:
        #    with open(syncSchedule, 'w') as GDBlog:
        #        GDBlog.write(str(2))
        #        GDBlog.close()
        #else:
        #    with open(syncSchedule, 'w') as GDBlog:
        #        GDBlog.write(str(1))
        #        GDBlog.close()
##-------------------------------------------------------------------------------
        logMessage("Preparing to update " + FStitle + " Feature Service")
        updateFS(myContent, version)


    except:
            # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
            print(pymsg)

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
                                            exportParameters=lllyyrrss,
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
            # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
            print(pymsg)

def publishFeatureService(gdbID):
    try:
        logMessage("Publishing " + FStitle + "feature service")
        print("Publishing " + FStitle + "feature service")
        # Publish FS Logic
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        print(description)
        publishParams = arcrest.manageorg.PublishFGDBParameter(name=FStitle,
            layerInfo=li,
            description=description,
            maxRecordCount=-1,
            copyrightText=licenseInfo,
            targetSR=102100)
        content = org.content
        usercontent = content.users.user(username)
        if isinstance(usercontent, manageorg.administration._content.User):
            pass
        result = usercontent.publishItem(fileType="fileGeodatabase", publishParameters=publishParams, itemId=gdbID, wait=True)
        print(result)
        logMessage(FStitle + " feature service publishing complete... preparing to export to a feature collection.")
        exportFeatureCollection(result.item.id)

    except:
        tb = sys.exc_info()[2]
        tbinfo = traceback.format_tb(tb)[0]
        pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
        logMessage("" + pymsg)
        print(pymsg)

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

        fs = os.path.getsize(fgdb1)

        #TODO add check for file size...if larger than 100 MBs we should set multipart to true
        #see ArcREST _content.addItem
        result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb1)
        #usercontent.addItem(itemParameters=itemParams, filePath=r"D:\Solutions\511\SyncFeaturecollection\SyncFeaturecollection\PublicationData\RFC5112.gdb.zip")

        #if 'success' in result:
        fgdb_itemID = result.id
        logMessage(FStitle + " File Geodatabase upload completed... preparing to publish as a feature service.")
        publishFeatureService(fgdb_itemID)
        #else:
        #    exit

        # end of script----------------------------------------------------------------------
    except:
            # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
            print(pymsg)

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
        #uploadFGDB()

    except:
            # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
            print(pymsg)

def getUserContent():
    content = org.content
    usercontent = content.users.user(username)

def main():
    """ main driver of program """
    sync_Init()

if __name__ == "__main__":
    main()