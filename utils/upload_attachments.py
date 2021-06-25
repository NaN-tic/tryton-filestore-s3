#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
upload_attachments_to_s3
Uploads the existing attachments in a data folder to S3
Original Code From:
    https://github.com/trytonus/trytond-attachment-s3/blob/develop/utils/upload_attachments_to_s3.py
"""
import os
from optparse import OptionParser
import boto3


def upload(client, bucket, data_path, database, new_db_name=None):
    """
    :param client: The Amazon S3 connection
    :param bucket: The name of the bucket to which the data has to be uploaded
    :param data_path: The data_path used by Tryton
    :param database: The name of the database
    :param new_db_name: If the db name on new infrastructure is different.
                        This DB name will be used as prefix instead of the
                        existing db name.
    """
#    bucket = connection.get_bucket(bucket)

    if new_db_name is None:
        new_db_name = database

    counter = {
        'ignored': 0,
        'uploaded': 0,
        'directories': 0,
    }

    print(os.path.join(data_path, database))
    for dirpath, _dn, filenames in os.walk(os.path.join(data_path, database)):
        counter['directories'] += 1
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            key = "%s/%s" % (new_db_name, filename)
            client.upload_file(filepath, bucket, key,
                ExtraArgs={'StorageClass': 'STANDARD_IA'})
            counter['uploaded'] += 1
            print( "[%d]: File %s uploaded to S3" % (
                counter['uploaded'], filename)
            )
    print( """Operation Completed
    Uploaded Documents: %(uploaded)d
    Ignored Documents: %(ignored)d
    Directories Traversed: %(directories)d
    """ % (counter))

if __name__ == '__main__':
    parser = OptionParser(
        usage="usage: %prog [options]" + (
            " access_key secret_key bucket data_path db_name"
        )
    )
    parser.add_option("-n", "--new-db-name", dest="new_db_name",
        help="New database name to x to the files", default=None)

    (options, args) = parser.parse_args()

    if not len(args) == 5:
        parser.error("Invalid options")

    REGION_HOST = 's3.eu-west-3.amazonaws.com'

    client = boto3.client('s3',
        aws_access_key_id = args[0],
        aws_secret_access_key = args[1])
    upload(client, new_db_name=options.new_db_name, *args[2:])
