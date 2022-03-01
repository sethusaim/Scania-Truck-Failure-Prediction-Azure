import json
import os
import pickle
from io import StringIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from utils.logger import app_logger
from utils.model_utils import get_model_name
from utils.read_params import read_params


class s3_operations:
    """
    Description :   This method is used for all the S3 bucket operations

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self):
        self.log_writer = app_logger()

        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.file_format = self.config["model_utils"]["save_format"]

    def get_s3_client(self, table_name):
        """
        Method Name :   get_s3_client
        Description :   This method is used getting the s3 client from AWS

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_s3_client.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            s3_client = boto3.client("s3")

            self.log_writer.log(
                table_name=table_name, log_message=f"Got s3 client from AWS"
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return s3_client

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def get_s3_resource(self, table_name):
        """
        Method Name :   get_s3_resource
        Description :   This method is used getting the s3 resource from AWS

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_s3_resource.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            s3_resource = boto3.resource("s3")

            self.log_writer.log(
                table_name=table_name, log_message=f"Got s3 client from AWS"
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return s3_resource

        except Exception as e:
            raise e

    def read_object(self, obj, table_name, decode=True, make_readable=False):
        """
        Method Name :   read_object
        Description :   This method is used reading the s3 object

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.read_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            func = (
                lambda: obj.get()["Body"].read().decode()
                if decode is True
                else obj.get()["Body"].read()
            )

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Read the s3 object with decode as {decode}",
            )

            conv_func = lambda: StringIO(func()) if make_readable is True else func()

            self.log_writer.log(
                table_name=table_name,
                log_message=f"read the s3 object with make_readable as {make_readable}",
            )

            return conv_func()

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def read_text(self, file_name, bucket_name, table_name):
        """
        Method Name :   read_json
        Description :   This method is used for loading a json file from s3 bucket (schema file)

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.read_text.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            txt_obj = self.get_file_object(
                bucket=bucket_name, filename=file_name, table_name=table_name
            )

            content = self.read_object(obj=txt_obj, table_name=table_name)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Read {file_name} file as text from {bucket_name} bucket",
            )

            return content

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def read_json(self, bucket, filename, table_name):
        """
        Method Name :   read_json
        Description :   This method is used for loading a json file from s3 bucket (schema file)

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.read_json.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            f_obj = self.get_file_object(
                bucket=bucket, filename=filename, table_name=table_name
            )

            json_content = self.read_object(obj=f_obj, table_name=table_name)

            dic = json.loads(json_content)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Read {filename} from {bucket} bucket",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return dic

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def get_df_from_object(self, obj, table_name):
        """
        Method Name :   get_df_from_object
        Description :   This method is used for converting object to dataframe

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        try:
            content = self.read_object(obj, table_name=table_name, make_readable=True)

            df = pd.read_csv(content)

            return df

        except Exception as e:
            raise e

    def read_csv(self, bucket, file_name, table_name, folder=False):
        """
        Method Name :   read_csv
        Description :   This method is used for reading the csv file from s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.read_csv.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            csv_obj = self.get_file_object(
                bucket=bucket, filename=file_name, table_name=table_name
            )

            if folder is True:
                lst = [
                    (
                        self.get_df_from_object(obj=obj, table_name=table_name),
                        obj.key,
                        obj.key.split("/")[-1],
                    )
                    for obj in csv_obj
                ]

                return lst

            else:
                df = self.get_df_from_object(obj=csv_obj, table_name=table_name)

                return df

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def load_object(self, bucket_name, obj, table_name):
        """
        Method Name :   load_object
        Description :   This method is used for loading a object from s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.load_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            s3_resource = self.get_s3_resource(table_name=table_name)

            s3_resource.Object(bucket_name, obj).load()

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Loaded {obj} from {bucket_name} bucket",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def create_folder(self, bucket_name, folder_name, table_name):
        """
        Method Name :   create_folder
        Description :   This method is used for creating a folder in s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.create_folder.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            self.load_object(bucket_name=bucket_name, obj=folder_name)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Folder {folder_name} already exists.",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                self.log_writer.log(
                    table_name=table_name,
                    log_message=f"{folder_name} folder does not exist,creating new one",
                )

                self.put_object(
                    bucket=bucket_name, folder_name=folder_name, table_name=table_name,
                )

                self.log_writer.log(
                    table_name=table_name,
                    log_message=f"{folder_name} folder created in {bucket_name} bucket",
                )

            else:
                self.log_writer.log(
                    table_name=table_name,
                    log_message=f"Error occured in creating {folder_name} folder",
                )

                self.log_writer.exception_log(
                    error=e,
                    class_name=self.class_name,
                    method_name=method_name,
                    table_name=table_name,
                )

    def put_object(self, bucket, folder_name, table_name):
        """
        Method Name :   put_object
        Description :   This method is used for putting any object in s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.put_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            s3_client = self.get_s3_client(table_name=table_name)

            s3_client.put_object(Bucket=bucket, Key=(folder_name + "/"))

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Created {folder_name} folder in {bucket} bucket",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def upload_file(self, src_file, table_name, bucket, dest_file, remove=True):
        """
        Method Name :   upload_file
        Description :   This method is used for uploading the files to s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            self.log_writer.log(
                table_name=table_name,
                log_message=f"Uploading {src_file} to s3 bucket {bucket}",
            )

            s3_resource = self.get_s3_resource(table_name=table_name)

            s3_resource.meta.client.upload_file(src_file, bucket, dest_file)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Uploaded {src_file} to s3 bucket {bucket}",
            )

            if remove:
                self.log_writer.log(
                    table_name=table_name,
                    log_message=f"Option remove is set {remove}..deleting the file",
                )

                os.remove(src_file)

                self.log_writer.log(
                    table_name=table_name,
                    log_message=f"Removed the local copy of {src_file}",
                )

                self.log_writer.start_log(
                    key="exit",
                    class_name=self.class_name,
                    method_name=method_name,
                    table_name=table_name,
                )

            else:
                self.log_writer.log(
                    table_name=table_name,
                    log_message=f"Option remove is set {remove}, not deleting the file",
                )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def get_bucket(self, bucket, table_name):
        """
        Method Name :   get_bucket
        Description :   This method is used for getting the bucket from s3

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_bucket.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            s3_resource = self.get_s3_resource(table_name=table_name)

            bucket = s3_resource.Bucket(bucket)

            self.log_writer.log(
                table_name=table_name, log_message=f"Got {bucket} bucket",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return bucket

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def copy_data(self, src_bucket, src_file, dest_bucket, dest_file, table_name):
        """
        Method Name :   copy_data
        Description :   This method is used for copying the data from one bucket to another

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.copy_data.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            copy_source = {"Bucket": src_bucket, "Key": src_file}

            s3_resource = self.get_s3_resource(table_name=table_name)

            s3_resource.meta.client.copy(copy_source, dest_bucket, dest_file)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Copied data from bucket {src_bucket} to bucket {dest_bucket}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def delete_file(self, bucket, file, table_name):
        """
        Method Name :   delete_file
        Description :   This method is used for deleting any file from s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.delete_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            s3_resource = self.get_s3_resource(table_name=table_name)

            s3_resource.Object(bucket, file).delete()

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Deleted {file} from bucket {bucket}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def move_data(self, src_bucket, src_file, dest_bucket, dest_file, table_name):
        """
        Method Name :   move_data
        Description :   This method is used for moving the data from one bucket to another bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.move_data.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            self.copy_data(
                src_bucket=src_bucket,
                src_file=src_file,
                dest_bucket=dest_bucket,
                dest_file=dest_file,
                table_name=table_name,
            )

            self.delete_file(
                bucket=src_bucket, file=src_file, table_name=table_name,
            )

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Moved {src_file} from bucket {src_bucket} to {dest_bucket}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def get_files(self, bucket, folder_name, table_name):
        """
        Method Name :   get_files
        Description :   This method is used for getting the file names from s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_files.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            lst = self.get_file_object(
                bucket=bucket, table_name=table_name, filename=folder_name,
            )

            list_of_files = [obj.key for obj in lst]

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Got list of files from bucket {bucket}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return list_of_files

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def get_file_object(self, bucket, filename, table_name):
        """
        Method Name :   get_file_object
        Description :   This method is used for getting file contents from s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_file_object.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            s3_bucket = self.get_bucket(bucket=bucket, table_name=table_name,)

            lst_objs = [obj for obj in s3_bucket.objects.filter(Prefix=filename)]

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Got {filename} from bucket {bucket}",
            )

            func = lambda x: x[0] if len(x) == 1 else x

            file_objs = func(lst_objs)

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return file_objs

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def load_model(self, bucket, model_name, table_name):
        """
        Method Name :   load_model
        Description :   This method is used for loading the model from s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.load_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            f_obj = self.get_file_object(
                bucket=bucket, filename=model_name, table_name=table_name,
            )

            model_obj = self.read_object(f_obj, decode=False, table_name=table_name)

            model = pickle.loads(model_obj)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Loaded {model_name} from bucket {bucket}",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return model

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def save_model(self, idx, model, model_bucket, table_name, model_dir):
        """
        Method Name :   save_model
        Description :   This method is used for saving a model to s3 bucket

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.save_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            model_name = get_model_name(model=model, table_name=table_name)

            func = (
                lambda: model_name + self.file_format
                if model_name == "KMeans"
                else model_name + str(idx) + self.file_format
            )

            model_file = func()

            with open(file=model_file, mode="wb") as f:
                pickle.dump(model, f)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Saved {model_name} model as {model_file} name",
            )

            s3_model_path = model_dir + "/" + model_file

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Uploading {model_file} to {model_bucket} bucket",
            )

            self.upload_file(
                src_file=model_file,
                bucket=model_bucket,
                dest_file=s3_model_path,
                table_name=table_name,
            )

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Uploaded  {model_file} to {model_bucket} bucket",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

            return "success"

        except Exception as e:
            self.log_writer.log(
                table_name=table_name,
                log_message=f"Model file {model_name} could not be saved",
            )

            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

    def upload_df_as_csv(
        self, data_frame, file_name, bucket, dest_file_name, table_name
    ):
        """
        Method Name :   upload_df_as_csv
        Description :   This method is used for uploading a dataframe to s3 bucket as csv file

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.upload_df_as_csv.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=table_name,
        )

        try:
            data_frame.to_csv(file_name, index=None, header=True)

            self.log_writer.log(
                table_name=table_name,
                log_message=f"Created a local copy of dataframe with name {file_name}",
            )

            self.upload_file(
                src_file=file_name,
                bucket=bucket,
                dest_file=dest_file_name,
                table_name=table_name,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=table_name,
            )
