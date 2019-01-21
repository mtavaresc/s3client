from configparser import ConfigParser

from boto3.session import Session


class S3Client:
    def __init__(self, __config_path):
        self.__config_path = __config_path

        config = ConfigParser()
        config.read(__config_path)

        session = Session(aws_access_key_id=config.get('Credentials', 'aws_access_key_id'),
                          aws_secret_access_key=config.get('Credentials', 'aws_secret_access_key'))

        self.s3_client = session.client('s3')
        self.s3_resource = session.resource('s3')

    def uploader(self, filename, bucket, key):
        try:
            self.s3_client.upload_file(filename, bucket, key)
        except Exception as e:
            print(format(e))
            return False
        return True

    def downloader(self, bucket, key, filename):
        try:
            self.s3_client.download_file(bucket, key, filename)
        except Exception as e:
            print(format(e))
            return False
        return True

    def delete_all_objects(self, bucket_name):
        res = []
        bucket = self.s3_resource.Bucket(bucket_name)
        try:
            for obj in bucket.object_versions.all():
                res.append({'Key': obj.object_key, 'VersionId': obj.id})

            bucket.delete_objects(Delete={'Objects': res})
        except Exception as e:
            print(format(e))
            return False
        return True

    def list_all_objects(self, bucket_name):
        res = []
        try:
            for key in self.s3_client.list_objects(Bucket=bucket_name)['Contents']:
                res.append(key['Key'])
        except Exception as e:
            print(format(e))
            return False
        return res
