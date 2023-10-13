import datetime
from collections import defaultdict


class Rewinder:
    def __init__(self, client, timestamp):
        self.client = client  # Minio(...)
        my_year = int(timestamp[0:4])
        my_month = int(timestamp[5:7])
        my_day = int(timestamp[8:10])
        my_hour = int(timestamp[11:13])
        my_minute = int(timestamp[14:16])
        self.rewind = datetime.datetime(my_year, my_month, my_day, my_hour, my_minute)

    def download(self, bucket, path):
        # Get list of all object-versions from Minio
        object_list = self.client.list_objects(
            bucket, path, recursive=True, include_version=True
        )

        # Filter on versions from before the rewind datetime
        object_list_before = filter(
            lambda o: o._last_modified.replace(tzinfo=None) < self.rewind, object_list
        )

        # For every object name, store the remaining versions
        object_versions = defaultdict(list)

        for version in list(object_list_before):
            object_versions[version._object_name].append(version)

        # Array of object version that we want to download
        downloads = []

        # We only want the latest version, and only if it was not deleted
        for object_name, versions in object_versions.items():
            versions.sort(key=lambda version: version._last_modified, reverse=True)
            if not versions[0]._is_delete_marker:
                downloads.append(
                    {"object_name": object_name, "version_id": versions[0]._version_id}
                )

        # Download to the same local path as the object path
        for download in downloads:
            print("Downloading:", download["object_name"])
            self.client.fget_object(
                bucket,
                download["object_name"],
                download["object_name"],
                version_id=download["version_id"],
            )
