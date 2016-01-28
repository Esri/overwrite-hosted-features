import arcpy, os, zipfile, time
from ConfigParser import ConfigParser

arcpy.env.OverwriteOutput = True

config = ConfigParser()
config.readfp(open(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'SyncFeatureCollection.cfg')))
fgdb1 = os.path.normpath(config.get('Data Sources', 'fgdb1'))

gdb_path = r"D:\Solutions\511\SyncFeaturecollection\SyncFeaturecollection\TempData\RFC511.gdb"
fc_name = "RandomPoints"
constraining_fc = r"D:\Solutions\511\SyncFeaturecollection\SyncFeaturecollection\New File Geodatabase.gdb\World"
#constraining_fc = r"D:\Solutions\511\SyncFeaturecollection\SyncFeaturecollection\New File Geodatabase.gdb\US"
#constraining_fc = r"D:\Solutions\511\SyncFeaturecollection\SyncFeaturecollection\New File Geodatabase.gdb\CO"
#constraining_fc = r"D:\Solutions\511\SyncFeaturecollection\SyncFeaturecollection\New File Geodatabase.gdb\Denver"

num_pts = [5, 100, 2000]

def zip_dir(path, ziph):
    isdir = os.path.isdir
    #for root, dirs, files in os.walk(path):
    #    for file in files:
    #        print(file)
    #        p = os.path.join(root, file)
    #        print(p)
    #        ziph.write(p)
    for each in os.listdir(path):
        fullname = path + "/" + each
        if not isdir(fullname):
            # If the workspace is a file geodatabase, avoid writing out lock
            # files as they are unnecessary
            if not each.endswith('.lock'):
                # Write out the file and give it a relative archive path
                try: 
                    ziph.write(fullname, each)
                except IOError: 
                    None # Ignore any errors in writing file
        else:
            # Branch for sub-directories
            for eachfile in os.listdir(fullname):
                if not isdir(eachfile):
                    if not each.endswith('.lock'):
                        try: 
                            ziph.write(fullname + "/" + eachfile, os.path.basename(fullname) + "/" + eachfile)
                        except IOError: 
                            None # Ignore any errors in writing file

def t(i):
    i+=1
    if i == 3:
        i = 0

    time.sleep(120)
    main(i)

def main(i):
    x=i
    out_path = os.path.join(gdb_path, fc_name)
    if arcpy.Exists(out_path):
        arcpy.Delete_management(out_path)
        print("Old points deleted")

    #create random points
    arcpy.CreateRandomPoints_management(gdb_path, fc_name, constraining_fc, "", num_pts[x], "", "POINT", "")
    print("New points created")
    

    #zip the GDB
    p = os.path.join(os.path.abspath(os.path.dirname(__file__)), os.path.basename(fgdb1))
    zip_file = zipfile.ZipFile(p, 'w', zipfile.ZIP_DEFLATED)
    zip_dir(os.path.dirname(gdb_path), zip_file)
    zip_file.close()
    print("ZipFile created")

    #replace old zip
    if arcpy.Exists(fgdb1):
        os.remove(fgdb1)

    
    os.rename(p, fgdb1)
    print("Data copied: " + fgdb1)
    t(i)

if __name__ == "__main__":
    try:
        main(0)
    except Exception, ex:
        print(ex.args)
