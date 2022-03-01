import os
from scania.blob_storage_operations.blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.read_params import read_params


class Azure_Container:
    def __init__(self):
        self.config = read_params()

        self.connection_string = os.environ["AZURE_CONN_STR"]

        self.containers = list(self.config["container"].values())

        self.blob = Blob_Operation()

        self.class_name = self.__class__.__name__

        self.db_name = self.config["db_log"]["train"]

        self.collection_name = self.config["train_db_log"]["general"]

        self.log_writer = App_Logger()

    def generate_containers(self):
        """
        Method Name :   generate_container
        Description :   This method is used for creating the container for log

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.generate_containers.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            for container in self.containers:
                self.blob.create_container(
                    container_name=container,
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )
