#*******************************************************************************
# Name:        SyncFeatureCollection
# Purpose:     Pushes edits from a hosted feature service to a feature
#              collection.  This supports workflows that prohibit the hosted
#              feature service from being exposed because it is edited
#              internally.  The feature collection can be created and exsposed
#              to public facing applications with out fear of modification by
#              unauthorized editors.  The feature collection is highly scalable
#              because it is a json file stored in Amazon S3 so hundreds of
#              thousands of users can utilize it without degrading performance.
#
# Author:      Eric J. Rodenberg, Solution Engineer Transportation Practice
#
# Created:     January 20, 2015
# Version:     2.7.8 (default, Jun 30 2014, 16:03:49) [MSC v.1500 32 bit (Intel)]

# Copyright 2015-2025 ESRI.
# All rights reserved under the copyright laws of the United States.
# You may freely redistribute and use this sample code, with or without
# modification. The sample code is provided without any technical support or
# updates.
#
# Disclaimer OF Warranty: THE SAMPLE CODE IS PROVIDED "AS IS" AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING THE IMPLIED WARRANTIES OF MERCHANTABILITY
# FITNESS FOR A PARTICULAR PURPOSE, OR NONINFRINGEMENT ARE DISCLAIMED. IN NO
# EVENT SHALL ESRI OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
# PROFITS; OR BUSINESS INTERRUPTION) SUSTAINED BY YOU OR A THIRD PARTY, HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT ARISING IN ANY WAY OUT OF THE USE OF THIS SAMPLE CODE, EVEN IF ADVISED
# OF THE POSSIBILITY OF SUCH DAMAGE. THESE LIMITATIONS SHALL APPLY
# NOTWITHSTANDING ANY FAILURE OF ESSENTIAL PURPOSE OF ANY LIMITED REMEDY.
#
# For additional information contact:
#
# Environmental Systems Research Institute, Inc.
# Attn: Contracts Dept.
# 380 New York Street
# Redlands, California, U.S.A. 92373
# Email: contracts@esri.com
#*******************************************************************************

import datetime, json, os, sys, subprocess, time, traceback, uuid, urllib
from ConfigParser import ConfigParser
from subprocess import Popen
from arcrest import manageorg
import arcrest

# Read Configuration File
config = ConfigParser()
config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection.cfg')))

# Configuralbe Variables
# Application Program Title
appProgram = config.get("Application Program Title", "appProgram")

# Log file location
syncSchedule = os.path.normpath(config.get('Log File Location', 'syncSchedule'))
syncLOG = os.path.normpath(config.get('Log File Location', 'syncLog'))

# Data Source(s)
fgdb1 = os.path.normpath(config.get('Data Sources', 'fgdb1'))

#JSON Export File containing the updated feature collection layer definition.
jsonExport = os.path.normpath(config.get('JSON Export', 'jsonExport'))

# Organization URL
baseURL = config.get('Portal Sharing URL', 'baseURL') + "sharing/rest"

# portal credentials *Fianl Script will be configurable
username = config.get('Portal Credentials', 'username')
pw = config.get('Portal Credentials', 'pw')

# Item Title *Fianl Script will be configurable
FStitle = config.get('Item Title', 'FStitle')
FCtitle = config.get('Item Title', 'FCtitle')
FCtemp = config.get('Item Title', 'FCtemp')

# Item Description Info *Final script will be configurable
tags = config.get('Service Description Info', 'tags')
snippet = config.get('Service Description Info', 'snippet')
description = config.get('Service Description Info', 'description')
licenseInfo = config.get('Service Description Info', 'licenseInfo')
thumbnail = os.path.normpath(config.get('Service Description Info', 'thumbnail'))

updateInterval = int(config.get('Update Interval', 'updateInterval'))* 60

#-------------------------------------------------------------------------------
# Fixed Variables
# Item Types
syncGDB = None
gdbType = "File Geodatabase"
fcType = "Feature Collection"
starttime = None
#-------------------------------------------------------------------------------
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
#--------------------------------------------------------------------------
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
#-------------------------------------------------------------------------------

def logMessage(myMessage):
        # Close out the log file
        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log = open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "SyncLog", "SyncLog.txt"),"a")
        log.write("     " + str(d) + " - " +myMessage + "\n")
        log.close()
#-------------------------------------------------------------------------------
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
#-------------------------------------------------------------------------------
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
#-------------------------------------------------------------------------------
def timer():
    while True:
        time.sleep(updateInterval)
        Popen([os.path.join(os.path.abspath(sys.exec_prefix),'python.exe'), os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection.py')])
        sys.exit(0)
#-------------------------------------------------------------------------------
def updateProductionFC(productionDict, tempFC_ID):
    try:
        #Start Logging
        logMessage("Update " + FCtitle + " production feature collection")

        #Update Logic Begins here...
        sh = arcrest.AGOLTokenSecurityHandler(username=username, password=pw)
        admin = manageorg.Administration(url=baseURL, securityHandler=sh)
        gdb_itemId = productionDict['Feature Collection']
        content = admin.content
        item = content.item(gdb_itemId)
        usercontent = content.usercontent(username)

        #Get JSON file to be passed into the production Feature Collection.
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

        print "Updating production Feature Collection"
        result = usercontent.updateItem(itemId=gdb_itemId, updateItemParameters=itemParams, text=updatedFeatures, async=False, overwrite=True)



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
                os.remove(fGDB)
                logMessage(FCtitle + " was successfully updated!")
                loggingEnd("Drive Texas was successully updated!")
                print "Sync complete, Feature Collection updated"
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
#-------------------------------------------------------------------------------
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
        usercontent = content.usercontent(username)
        #Get the Updated Feature Service item id
        fsID = myContentDict['Feature Service']
        if isinstance(usercontent, manageorg.administration._content.UserContent):
                pass
        result = usercontent.exportItem(title=FCtemp,
                                            itemId=fsID,
                                            exportFormat=fcType,
                                            exportParameters={"layers":[{"id":1},{"id":0}]})


        exportedItemId = result['exportItemId']
        jobId = result['jobId']
        exportItem = content.item(itemId=exportedItemId)
        #   Ensure the item is finished exporting before downloading
        #
        print usercontent.status(itemId=exportedItemId, jobId=jobId, jobType="export")
        status =  usercontent.status(itemId=exportedItemId, jobId=jobId, jobType="export")

        print "Update Temporary Feature Collection"
        print status
        while status['status'].lower() == "processing":
            time.sleep(3)
            print status
            status =  usercontent.status(itemId=exportedItemId,
                                        jobId=jobId,
                                        jobType="export")

        #Export Temporary Feature Collection as in memory JSON response
        #jsonExport = exportItem.itemData(f="json")
        token = sh.token
        url = baseURL + "/content/items/" + exportedItemId + "/data?token=" + token + "&f=json"
        response = urllib.urlretrieve(url, jsonExport)
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
#-------------------------------------------------------------------------------
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
                layerInfo={"serviceDescription": "Current road conditions as of " + str(d ),"capabilities":"Query","layers":[{"id":0, "name":"Current Conditions","geometryType":"esriGeometryPoint","minScale":0,"maxScale":0,"drawingInfo":{"renderer":{"type":"simple","symbol":{"type":"esriSMS","style":"esriSMSCircle","color":[0,0,0,255],"size":18,"angle":0,"xoffset":0,"yoffset":0},"label":"","description":""}}},
                            {"id":1,"name":"Current Segment Conditions", "geometryType":"esriGeometryPolyline","minScale":0,"maxScale":320000,"drawingInfo":{"renderer":{"type":"simple","symbol":{"type":"esriSLS","style":"esriSLSSolid","color":[242,125,71,255],"width":1,"outline":{"color":[117,116,115,255],"width":1}},"label":"","description":""}}}]},
                maxRecordCount=-1,
                copyrightText=licenseInfo,
                targetSR=102100)

        gdbID = contentDict['File Geodatabase']
        content = org.content
        usercontent = content.usercontent(username)
        if isinstance(usercontent, manageorg.administration._content.UserContent):
                pass
        result = usercontent.publishItem(itemId=gdbID, fileType="fileGeodatabase", publishParameters=publishParams, overwrite=True )

        listResult = result['services']
        dictResult = listResult.pop()
        publishedItemId = dictResult['serviceItemId']
        jobId = dictResult['jobId']

        publishItem = content.item(itemId=publishedItemId)
        #   Ensure the item is finished exporting before downloading
        #
        status =  usercontent.status(itemId=publishedItemId, jobId=jobId, jobType="publish")

        print "Update Feature Service"
        print status
        while status['status'].lower() == "processing":
            time.sleep(3)
            print status
            status =  usercontent.status(itemId=publishedItemId,
                                        jobId=jobId,
                                        jobType="publish")
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
#-------------------------------------------------------------------------------
def updateFGDB(myContent):
    try:
        #Log startup message
        logMessage("Updating " + FStitle + " File Geodatabase item with new data")
        version = None
        #Check to see which FGDB updated last. Final script will look for a folder
##-------------------------------------------------------------------------------
        #This block is for testing only... switching back and forth between two sample Geodatabases
        #try:
            #with open(syncSchedule, 'r') as GDBlog:
                #try:
                   #lastGDBused = int(GDBlog.readline())
                   #if lastGDBused == 1:
                        #fGDB = fgdb2
                        #version = 2
                   #elif lastGDBused == 2:
                        #fGDB = fgdb1
                        #version = 1
                #except:
                    #lastGDBused = 0

        #except IOError:
                #with open(os.path.join(os.path.abspath(os.path.dirname(__file__)), "SyncSchedule.txt"),"w") as GDBlog:
                    #GDBlog.write(str(1))
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
        item = content.item(gdb_itemId)
        usercontent = content.usercontent(username)

        d = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        itemParams = manageorg.ItemParameter()
        itemParams.title=FStitle
        itemParams.description="Updated " + str(d) + "\n GDB Version " + str(version)

        usercontent.updateItem(itemId=gdb_itemId, updateItemParameters=itemParams, filePath=fGDB, async=True, overwrite=True)

        status =  usercontent.status(itemId=gdb_itemId, jobType="update")
        print "Updating File Geodatabase"
        print status
        while status['status'].lower() == "processing":
            time.sleep(3)
            print status
            status =  usercontent.status(itemId=gdb_itemId,
                                        jobType="update")

        logMessage(FStitle + "File Geodatabase Updated!")

##-------------------------------------------------------------------------------
        # Temporary code for Testing To Be Removed at Final
        # Update the sync schedule file
        if lastGDBused == 1:
            with open(syncSchedule, 'w') as GDBlog:
                GDBlog.write(str(2))
                GDBlog.close()
        else:
            with open(syncSchedule, 'w') as GDBlog:
                GDBlog.write(str(1))
                GDBlog.close()
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
#-------------------------------------------------------------------------------
def exportFeatureCollection(fsID):
    try:
        #Start Logging
        logMessage("Exporting " + FStitle + " feature service to a feature collection")
        print "Exporting " + FStitle + " feature service to a feature collection"
        # Publish FS Logic
        # Create security handler
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = manageorg.Administration(url=baseURL, securityHandler=sh)

        content = org.content
        usercontent = content.usercontent(username)
        if isinstance(usercontent, manageorg.administration._content.UserContent):
                pass
        result = usercontent.exportItem(title=FCtitle,
                                            itemId=fsID,
                                            exportFormat=fcType,
                                            exportParameters={"layers" : [{"id" : 1},{"id" : 0}]})


        exportedItemId = result['exportItemId']
        jobId = result['jobId']
        exportItem = content.item(itemId=exportedItemId)
        #   Ensure the item is finished exporting before downloading
        #
        status =  usercontent.status(itemId=exportedItemId, jobId=jobId, jobType="export")

        while status['status'].lower() == "processing":
            time.sleep(3)
            status =  usercontent.status(itemId=exportedItemId,
                                        jobId=jobId,
                                        jobType="export")

        # Set the itemParameters
        itemParams = manageorg.ItemParameter()
        itemParams.title = FCtitle
        itemParams.tags = tags
        itemParams.thumbnail = thumbnail
        itemParams.description = description
        itemParams.snippet = snippet
        itemParams.licenseInfo = licenseInfo

        productionFC = usercontent.updateItem(itemId=exportedItemId, updateItemParameters=itemParams)

        if 'success' in productionFC:
            logMessage(FCtitle + " feature collection created.")
            loggingEnd("Drive Texas Data Upload Complete!")
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
#-------------------------------------------------------------------------------
def publishFeatureService(gdbID):
    try:
        #Start Logging
        logMessage("Publishing " + FStitle + "feature service")
        print "Publishing " + FStitle + "feature service"
        # Publish FS Logic
        # Create security handler
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = manageorg.Administration(url=baseURL, securityHandler=sh)

        publishParams = arcrest.manageorg.PublishFGDBParameter(name=FStitle,
                    layerInfo={"capabilities": "Query","layers": [{"id": 0,"name": "CurrentConditionsAllPoints","geometryType": "esriGeometryPoint"}, {"id": 1,"name": "CurrentHighwayConditions_Line","geometryType": "esriGeometryPolyline"}]},
                    description=description,
                    maxRecordCount=-1,
                    copyrightText=licenseInfo,
                    targetSR=102100)
        content = org.content
        usercontent = content.usercontent(username)
        if isinstance(usercontent, manageorg.administration._content.UserContent):
                pass
        result = usercontent.publishItem(fileType="fileGeodatabase", publishParameters=publishParams, itemId=gdbID)
        print result
        response = result['services'].pop(-1)
        publishedItemId = response['serviceItemId']
        jobId = response['jobId']
        publishItem = content.item(itemId=publishedItemId)
        #   Ensure the item is finished exporting before downloading
        #
        status =  usercontent.status(itemId=publishedItemId, jobId=jobId, jobType="publish")

        while status['status'].lower() == "processing":
            time.sleep(3)
            status =  usercontent.status(itemId=publishedItemId,
                                        jobId=jobId,
                                        jobType="publish")
        logMessage(FStitle + " feature service publishing complete... preparing to export to a feature collection.")
        exportFeatureCollection(publishedItemId)

    except:
            # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
#-------------------------------------------------------------------------------
def uploadFGDB():
    try:
        # Log Step
        logMessage("Uploading " + FStitle + " File Geodatabase")
        print "Uploading " + FStitle + " File Geodatabase"
        # Sync Logic
        # Create security handler
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = arcrest.manageorg.Administration(url=baseURL, securityHandler=sh)

        # Set the itemParameters
        itemParams = arcrest.manageorg.ItemParameter()
        itemParams.title = FStitle
        itemParams.type = gdbType
        itemParams.tags = tags
        itemParams.overwrite = "false",

        # Grab the user's content (items)
        content = org.content
        usercontent = content.usercontent(username)
        if isinstance(usercontent, arcrest.manageorg.administration._content.UserContent):
            pass

        result = usercontent.addItem(itemParameters=itemParams, filePath=fgdb1)

        if 'success' in result:
            fgdb_itemID = result['id']
            logMessage(FStitle + " File Geodatabase upload completed... preparing to publish as a feature service.")
            publishFeatureService(fgdb_itemID)
        else:
            exit

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
#-------------------------------------------------------------------------------
def sync_Init():
    try:
        # Start Logging
        loggingStart("Syncronize " + appProgram + " Web Application with updated data")
        # Check to see if File Geodatabase, Feature Service and Feature
        # Collection have been previously uploaded.
        sh = arcrest.AGOLTokenSecurityHandler(username, pw)
        # Connect to AGOL
        org = manageorg.Administration(url=baseURL, securityHandler=sh)
        result = org.query(q=appProgram.replace(" ",""),bbox=None)
        keyset = ['results']

        value = None
        for key in keyset:
            if key in result:
                value = result[key]
                if (value == []):
                    print "The query for " + appProgram.replace(" ","") + " came up with no results"
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

    except:
            # Get the traceback object
            tb = sys.exc_info()[2]
            tbinfo = traceback.format_tb(tb)[0]

            # Concatenate information together concerning
            # the error into a message string
            pymsg = "PYTHON ERRORS:\nTraceback info:\n" + tbinfo + "\nError Info:\n" + str(sys.exc_info()[1])
            # Python / Python Window
            logMessage("" + pymsg)
#-------------------------------------------------------------------------------
def main():
    """ main driver of program """
    while True:
        sync_Init()
#-------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

#--------------------------------------------------------------------------
