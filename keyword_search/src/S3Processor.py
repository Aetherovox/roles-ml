import boto3
import json
from botocore import exceptions


class S3Processor:
    def __init__(self):
        self.config = self.get_config()
        self.bucket = self.config['bucket']
        self.private_key = self.config['access_key']
        self.secret_key = self.config['secret_key']
        self.client = self.authenticate_client()
        self.resource = self.authenticate_resource()

    @staticmethod
    def get_config():
        with open("config/config.json", "r") as read_file:
            data = json.load(read_file)
        return data['s3']

    def authenticate_client(self):
        s3 = boto3.client(
            service_name='s3',
            aws_access_key_id=self.private_key,
            aws_secret_access_key=self.secret_key,
        )
        return s3

    def authenticate_resource(self):
        s3 = boto3.resource(
            service_name='s3',
            aws_access_key_id=self.private_key,
            aws_secret_access_key=self.secret_key,
        )
        return s3

    def upload_file(self, source_file, target_object, folder):
        """Upload a file to an S3 bucket
        :param source_file: File to upload
        :param target_object: S3 object name
        :param folder: s3 folder raw / staging / target
        :return: True if file was uploaded, else False
        """

        loc_folder = folder + '/'
        attempts = 0
        success = False
        while not success and attempts < 2:
            try:
                response = self.client.upload_file(source_file, self.bucket, loc_folder + target_object)
                success = True
                return True
            except exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                print('Attempt no.' + str(error_code) + ': Error code - ' + str(error_code))
                if error_code == '404':
                    attempts += 1
                    self.resource = self.authenticate_resource()
                return False

    def download_file(self, source_object, target_file):
        """download a file from an S3 bucket
        :param source_object: s3 object to be downloaded
        :param target_file: local file name to save object as
        :return: True if file was downloaded, else False
        """
        attempts = 0
        success = False
        while not success and attempts < 2:
            try:
                self.resource.Bucket(self.bucket).download_file(source_object, target_file)
                success = True
                return True
            except exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                print('Attempt no.' + str(error_code) + ': Error code - ' + str(error_code))
                if error_code == '404':
                    attempts += 1
                    self.resource = self.authenticate_resource()
                return False
