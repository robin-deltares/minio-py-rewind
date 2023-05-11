import datetime
from minio import Minio
from minio.error import InvalidResponseError
from collections import defaultdict

# For access
myMinioServer = 'my.minio.server'
myAccessKey   = 'my_access_key'
mySecretKey   = 'my_secret_key'

# The path that will be recursively downloaded
myBucketName = 'my_bucket_name'
myPathName   = 'my_path_name'
myRewind     = datetime.datetime(2023,5,11,18,0,0) # Should become a different notation: 2023.05.10T16:00

# Connect to Minio
client = Minio(myMinioServer,
               access_key=myAccessKey,
               secret_key=mySecretKey)

# Get list of all object-rivisions from Minio
objectList = client.list_objects(myBucketName,
                                 myPathName,
                                 recursive=True,
                                 include_version=True)

# Filter on versions from before a certain timestamp
before = filter(lambda o: o._last_modified.replace(tzinfo=None) < myRewind, objectList)

# Store collection of object names, with all remaining revisions
revisions = defaultdict(list)

# Add the version objects to the collection of the object name
for obj in list(before):
    revisions[obj._object_name].append(obj)

# Array of object revisions that we want to download
downloads=[]

# Get only the latest versions, and only if it was not deleted
for object_name,versions in revisions.items():
    versions.sort(key=lambda x: x._last_modified, reverse=True)
    if versions[0]._is_delete_marker != True:
        downloads.append({'object_name':object_name, 'version_id':versions[0]._version_id})

# Download the files
for download in downloads:
    print("Downloading:", download['object_name'])
    client.fget_object(
        myBucketName, download['object_name'], download['object_name'],
        version_id=download['version_id'],
    )

print("Downloading complete")
