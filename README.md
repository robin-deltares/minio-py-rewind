# minio-py-rewind
Python equivalent to: mc cp --recursive --rewind

Example implementation:

```
from minio import Minio
import minio_rewind

# For access
myMinioServer = 'my.minio.server'
myAccessKey   = 'my_access_key'
mySecretKey   = 'my_secret_key'

# The path that will be recursively downloaded
myBucketName = 'my_bucket_name'
myPathName   = 'my_path_name'
myRewind     = '2023.05.10T16:00' # Notation that mc uses

# Minio client connection
myClient = Minio(myMinioServer,
                 access_key=myAccessKey,
                 secret_key=mySecretKey)

# Prepare the rewind-settings
rewinder = minio_rewind.Rewinder(myClient,myRewind)

# Download the objects
rewinder.download(myBucketName,myPathName)
```
