import datetime
from minio import Minio
from collections import defaultdict

class Rewinder:

    def __init__(self,client,timestamp):
        self.client = client # Minio(...)
        myYear   = int(timestamp[0:4])
        myMonth  = int(timestamp[5:7])
        myDay    = int(timestamp[8:10])
        myHour   = int(timestamp[11:13])
        myMinute = int(timestamp[14:16])
        self.rewind = datetime.datetime(myYear,myMonth,myDay,myHour,myMinute)

    def download(self,
               bucket,
               path):

        # Get list of all object-versions from Minio
        objectList = self.client.list_objects(bucket,
                                         path,
                                         recursive=True,
                                         include_version=True)

        # Filter on versions from before the rewind datetime
        objectListBefore = filter(lambda o: o._last_modified.replace(tzinfo=None) < self.rewind, objectList)

        # For every object name, store the remaining versions
        objectVersions = defaultdict(list)

        for version in list(objectListBefore):
            objectVersions[version._object_name].append(version)

        # Array of object version that we want to download
        downloads=[]

        # We only want the latest version, and only if it was not deleted
        for object_name,versions in objectVersions.items():
            versions.sort(key=lambda version: version._last_modified, reverse=True)
            if versions[0]._is_delete_marker != True:
                downloads.append({'object_name':object_name, 'version_id':versions[0]._version_id})

        # Download to the same local path as the object path
        for download in downloads:
            print("Downloading:", download['object_name'])
            self.client.fget_object(
                bucket, download['object_name'], download['object_name'],
                version_id=download['version_id'],
            )
