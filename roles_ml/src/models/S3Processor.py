import boto3
from botocore import exceptions
import json
import pathlib


class S3Processor:
    def __init__(self):
        self.config = self.get_config()
        self.bucket = self.config['bucket']
        self.private_key = self.config['access_key']
        self.secret_key = self.config['secret_key']
        self.client = self.authenticate_client()
        self.resource = self.authenticate_resource()
        self.contents = self.client.list_objects(Bucket=self.bucket)['Contents']
        self.files = self.retrieve_file_list(self.contents)

    @staticmethod
    def get_config():
        with open("config/config.json", "r") as read_file:
            data = json.load(read_file)
        return data['s3']

    @staticmethod
    def retrieve_file_list(s3_contents):
        file_list = []
        for k in s3_contents:
            if '.html' in k['Key']:
                file_list.append(k['Key'])
        return file_list

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

    def upload_file(self, source_file, target_object):
        """Upload a file to an S3 bucket
        :param source_file: File to upload
        :param target_object: S3 object name
        :param folder: s3 folder raw / staging / target
        :return: True if file was uploaded, else False
        """
        attempts = 0
        success = False
        while not success and attempts < 2:
            try:
                self.client.upload_file(source_file, self.bucket, target_object)
                return True
            except exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                print('Attempt no.' + str(error_code) + ': Error code - ' + str(error_code))
                if error_code == '404':
                    attempts += 1
                    self.resource = self.authenticate_resource()
                return False

    # TODO: If file already exists in target we return
    def download_file(self, source_object):
        """download a file from an S3 bucket
        :param overwrite: 'Y'/'N' if the file already exists we exit, returning False unless overwrite is specified
        :param source_object: s3 object to be downloaded
            - this will also create a file path if one does not exist
        :return: True if file was downloaded, else False
        """
        attempts = 0
        success = False
        target_file = "./files/" + source_object
        print("Target file is: {}".format(target_file))

        # define and create local directory to mirror the S3 bucket
        local_path = pathlib.Path(target_file).parent
        pathlib.Path(local_path).mkdir(parents=True, exist_ok=True)
        while not success and attempts < 2:
            try:
                self.resource.Bucket(self.bucket).download_file(source_object, target_file)
                success = True
                return success
            except exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                print('File: {} ; Attempt: {} ; Error code: {} '.format(source_object, attempts, error_code))
                if error_code == '404':
                    attempts += 1
                    self.resource = self.authenticate_resource()
                return success

    # TODO: ideally want keyword parameters to pull in only certain types of files or from certain directories
    def download_all(self):
        for file in self.files:
            print(file)
            self.download_file(file)
