# Imports
import arcpy
try:
    from urllib2 import urlopen
except:
    from urllib.request import urlopen
import json
import os
import zipfile
import io
import datetime
from src.utils.within_conus import check_coords
from utils.get_file_paths import get_shakemap_dir


def check_for_shakemaps(mmi_threshold = 3):

    FilePath = get_shakemap_dir()
    new_shakemap_folders = []

    # Create variables
    pnt = arcpy.Point()

    FEEDURL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/significant_week.geojson' #Significant Events - 1 week
    #FEEDURL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_hour.geojson' #1 hour M4.5+
    #FEEDURL = 'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson' #1 day M4.5+

    # Get the list of event IDs in the current feed
    fh = urlopen(FEEDURL) #open a URL connection to the event feed.
    data = fh.read() #read all of the Data from that URL into a string
    fh.close()
    jdict = json.loads(data) #Parse that Data using the stdlib json module.  This turns into a Python dictionary.


    # Create list of files in current folder
    FileList = os.listdir(FilePath)

    # Check to see if any new events have been added. If so, run code. If not, break and exit.
    eqIDlist=[earthquake['id'] for earthquake in jdict['features']]

    # noinspection PyUnboundLocalVariable
    for earthquake in jdict['features']: #jdict['features'] is the list of events
        eventid = earthquake['id']

        #if earthquake['id'] == eventid:
        #for earthquake['id'] in earthquake['id']:
        eventurl = earthquake['properties']['detail'] #get the event-specific URL
        fh = urlopen(eventurl)
        data = fh.read() #read event Data into a string
        fh.close()
        jdict2 = json.loads(data) #and parse using json module as before
        if jdict2['properties']['mag'] < mmi_threshold:
            print('\nSkipping {}: mag < {}'.format(eventid, mmi_threshold))
            continue
        if not 'shakemap' in jdict2['properties']['products'].keys():
            print('\nSkipping {}: no shakemap available'.format(eventid))
            continue

        longlat = jdict2['geometry']['coordinates'][0:2]
        F = check_coords(longlat[1], longlat[0])
        if not F:
            print('\nSkipping {}: epicenter not in conus'.format(eventid))
            continue

        shakemap = jdict2['properties']['products']['shakemap'][0] #get the first shakemap associated with the event
        shapezipurl = shakemap['contents']['download/shape.zip']['url'] #get the download url for the shape zipfile.
        try:
            epicenterurl = shakemap['contents']['download/epicenter.kmz']['url']
        except:
            print('\nSkipping {}: no epicenter available'.format(eventid))
            continue



    ## EXTRACT SHAKEMAP ZIP FILE IN NEW FOLDER

        #Here, read the binary zipfile into a string
        fh = urlopen(shapezipurl)
        data = fh.read()
        fh.close()

        #Create a StringIO object, which behaves like a file
        stringbuf = io.BytesIO(data)
        eventdir = "{}\{}".format(FilePath,str(eventid))

        # Creates a new folder (called the eventid) if it does not already exist
        if not os.path.isdir(eventdir):
            os.mkdir(eventdir)
            print("\nNew EventID: {}".format(eventid))

            #Create a StringIO object, which behaves like a file
            stringbuf = io.BytesIO(data)
            eventdir = "{}\{}".format(FilePath,str(eventid))

            # Create a ZipFile object, instantiated with our file-like StringIO object.
            # Extract all of the Data from that StringIO object into files in the provided output directory.
            myzip = zipfile.ZipFile(stringbuf,'r',zipfile.ZIP_DEFLATED)
            myzip.extractall(eventdir)
            myzip.close()
            stringbuf.close()

            # Create feature class of earthquake info
            epiX = earthquake['geometry']['coordinates'][0]
            epiY = earthquake['geometry']['coordinates'][1]
            depth = earthquake['geometry']['coordinates'][2]
            title = str(earthquake['properties']['title'])
            mag = earthquake['properties']['mag']
            time = str(earthquake['properties']['time'])
            time_ = datetime.datetime.fromtimestamp(int(time[:-3])).strftime('%c')
            place = str(earthquake['properties']['place'])
            #felt = earthquake['properties']['felt']
            url = str(earthquake['properties']['url'])
            eventid = str(eventid)
            status = str(earthquake['properties']['status'])
            updated = str(earthquake['properties']['updated'])
            updated_ = datetime.datetime.fromtimestamp(int(updated[:-3])).strftime('%c')

            f = open(eventdir+"\\eventInfo.txt","w+")
            f.write("{}\r\n{}\r\n".format(status,updated))
            f.close()

            COMBO = (title,mag,time,place,depth,url,eventid)
            print('New event successfully downloaded: \n', COMBO)

            # Update empty point with epicenter lat/long
            pnt.X = epiX
            pnt.Y = epiY

            # Add fields to Epicenter shapefile
            arcpy.CreateFeatureclass_management(eventdir,"Epicenter","POINT","","","",4326)
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Title","TEXT","","","","Event")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Mag","FLOAT","","","","Magnitude")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Date_Time","TEXT","","","","Date/Time")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Place","TEXT","","","","Place")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Depth_km","FLOAT","","","","Depth (km)")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Url","TEXT","","","","Url")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"EventID","TEXT","","","","Event ID")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Status","TEXT","","","","Status")
            arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Updated","TEXT","","","","Updated")

            # Add earthquake info to Epicenter attribute table
            curs = arcpy.da.InsertCursor("{}\Epicenter.shp".format(eventdir),["Title","Mag","Date_Time","Place","Depth_km","Url","EventID","Status","Updated"])
            curs.insertRow((title,mag,time_,place,depth,url,eventid,status,updated_))
            del curs





            # Add XY point Data to Epicenter shapefile
            with arcpy.da.UpdateCursor("{}\Epicenter.shp".format(eventdir),"SHAPE@XY") as cursor:
                for eq in cursor:
                    eq[0]=pnt
                    cursor.updateRow(eq)
            del cursor

            filelist = os.listdir(eventdir)
            print('Extracted {} ShakeMap files to {}'.format(len(filelist),eventdir))
            new_shakemap_folders.append(eventdir)

        else:


            # go into folder and read former status and update time
            f = open(eventdir+"\\eventInfo.txt","r")
            oldstatus = f.readline()
            oldstatus = oldstatus.rstrip()
            oldupdated = f.readline() # this skips over the blank line between the status and update time
            oldupdated = f.readline()
            oldupdated = oldupdated.rstrip()
            f.close()


            status = str(earthquake['properties']['status'])
            updated = str(earthquake['properties']['updated'])

            # check to see if new dataset has been updated or has a new status
            t=1
            if status == oldstatus:
                t=0

            if int(updated) > int(oldupdated) or t==1:

                # create archive subdirectory
                list_subfolders = [f.name for f in os.scandir(eventdir) if f.is_dir()]
                olddate = datetime.datetime.fromtimestamp(int(oldupdated[:-3])).strftime('%Y%m%d')
                archive_folder_name = "archive_{}".format(olddate)
                archive_zip_name = archive_folder_name + ".zip"

                if not archive_zip_name in list_subfolders:
                    # copy all old files to new archive folder
                    archive_zip_fullpath = os.path.join(eventdir, archive_zip_name)
                    #os.mkdir(os.path.join(eventdir, archive_folder_name))
                    files_to_move = [f for f in os.listdir(eventdir) if os.path.isfile(os.path.join(eventdir, f))]
                    files_to_move.remove('eventInfo.txt')
                    # for f in files_to_move:
                    #     shutil.move(os.path.join(eventdir, f), os.path.join(eventdir, archive_folder_name))
                    with zipfile.ZipFile(archive_zip_fullpath, 'w') as zip:
                        for file in files_to_move:
                            zip.write(os.path.join(eventdir, file))
                    for filename in files_to_move:
                        os.remove(os.path.join(eventdir, filename))

                else:
                    # delete all old files if they have already been moved to archive folder
                    files_to_delete = [f for f in os.listdir(eventdir) if os.path.isfile(os.path.join(eventdir, f))]
                    files_to_delete.remove('eventInfo.txt')
                    for filename in files_to_delete:
                        os.remove(os.path.join(eventdir, filename))

                print("\nPreviously downloaded ShakeMap files for {} have been archived.".format(eventid))

                stringbuf = io.BytesIO(data)
                myzip = zipfile.ZipFile(stringbuf,'r',zipfile.ZIP_DEFLATED)
                myzip.extractall(eventdir)
                myzip.close()
                stringbuf.close()

                # Create feature class of earthquake info
                epiX = earthquake['geometry']['coordinates'][0]
                epiY = earthquake['geometry']['coordinates'][1]
                depth = earthquake['geometry']['coordinates'][2]
                title = str(earthquake['properties']['title'])
                mag = earthquake['properties']['mag']
                time = str(earthquake['properties']['time'])
                place = str(earthquake['properties']['place'])
                #felt = earthquake['properties']['felt']
                url = str(earthquake['properties']['url'])
                eventid = str(eventid)
                status = str(earthquake['properties']['status'])
                updated = str(earthquake['properties']['updated'])

                f = open(eventdir+"\\eventInfo.txt","w+")
                f.write("{}\r\n{}\r\n".format(status,updated))
                f.close()

                COMBO = (title,mag,time,place,depth,url,eventid)
                print('Updated event successfully downloaded: \n', COMBO)

                # Update empty point with epicenter lat/long
                pnt.X = epiX
                pnt.Y = epiY

                # Add fields to Epicenter shapefile
                arcpy.CreateFeatureclass_management(eventdir,"Epicenter","POINT","","","",4326)
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Title","TEXT","","","","Event")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Mag","FLOAT","","","","Magnitude")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Date_Time","TEXT","","","","Date/Time")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Place","TEXT","","","","Place")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Depth_km","FLOAT","","","","Depth (km)")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Url","TEXT","","","","Url")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"EventID","TEXT","","","","Event ID")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Status","TEXT","","","","Status")
                arcpy.AddField_management("{}\Epicenter.shp".format(eventdir),"Updated","TEXT","","","","Updated")

                # Add earthquake info to Epicenter attribute table
                curs = arcpy.da.InsertCursor("{}\Epicenter.shp".format(eventdir),["Title","Mag","Date_Time","Place","Depth_km","Url","EventID","Status","Updated"])
                curs.insertRow((title,mag,time,place,depth,url,eventid,status,updated))
                del curs


                # Add XY point Data to Epicenter shapefile
                with arcpy.da.UpdateCursor("{}\Epicenter.shp".format(eventdir),"SHAPE@XY") as cursor:
                    for eq in cursor:
                        eq[0]=pnt
                        cursor.updateRow(eq)
                del cursor

                filecount = [f for f in os.listdir(eventdir) if os.path.isfile(os.path.join(eventdir, f))]
                print('Successfully downloaded {} ShakeMap files to {}'.format(len(filecount),eventdir))
                new_shakemap_folders.append(eventdir)

            else:

                print("\nShakeMap files for {} already exist and have not been updated.".format(eventid))

    print("\nCompleted.")

    return new_shakemap_folders

if __name__ == "__main__":
    check_for_shakemaps()

