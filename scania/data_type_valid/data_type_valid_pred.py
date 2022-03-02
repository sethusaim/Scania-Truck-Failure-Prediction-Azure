from scania.blob_storage_operations.blob_operations import Blob_Operation
from scania.mongo_db_operations.mongo_operations import MongoDB_Operation
from utils.logger import App_Logger
from utils.read_params import read_params


class DB_Operation_Pred:
    """
    Description :    This class shall be used for handling all the db operations

    Version     :    1.2
    Revisions   :    moved setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.pred_data_container = self.config["container"]["scania_pred_data"]

        self.pred_export_csv_file = self.config["export_csv_file"]["pred"]

        self.good_data_pred_dir = self.config["data"]["pred"]["good"]

        self.input_files_container = self.config["container"]["input_files"]

        self.db_name = self.config["db_log"]["pred"]

        self.pred_db_insert_log = self.config["pred_db_log"]["db_insert"]

        self.pred_export_csv_log = self.config["pred_db_log"]["export_csv"]

        self.blob = Blob_Operation()

        self.mongo = MongoDB_Operation()

        self.log_writer = App_Logger()

    def insert_good_data_as_record(self, good_data_db_name, good_data_collection_name):
        """
        Method Name :   insert_good_data_as_record
        Description :   This method inserts the good data in MongoDB as collection

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.insert_good_data_as_record.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            collection_name=self.pred_db_insert_log,
        )

        try:
            lst = self.blob.read_csv_from_folder(
                folder_name=self.good_data_pred_dir,
                container_name=self.pred_data_container,
                db_name=self.db_name,
                collection_name=self.pred_db_insert_log,
            )

            for f in lst:
                df = f[1]

                file = f[2]

                if file.endswith(".csv"):
                    self.mongo.insert_dataframe_as_record(
                        data_frame=df,
                        db_name=good_data_db_name,
                        collection_name=good_data_collection_name,
                    )

                else:
                    pass

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.pred_db_insert_log,
                    log_info="Inserted dataframe as collection record in mongodb",
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_db_insert_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_db_insert_log,
            )

    def export_collection_to_csv(self, good_data_db_name, good_data_collection_name):
        """
        Method Name :   export_collection_to_csv

        Description :   This method extracts the inserted data to csv file, which will be used for preding
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.export_collection_to_csv.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            collection_name=self.pred_export_csv_log,
        )

        try:
            df = self.mongo.get_collection_as_dataframe(
                db_name=good_data_db_name,
                collection_name=good_data_collection_name,
            )

            self.blob.upload_df_as_csv(
                dataframe=df,
                local_file_name=self.pred_export_csv_file,
                container_file_name=self.pred_export_csv_file,
                container_name=self.input_files_container,
                db_name=self.db_name,
                collection_name=self.pred_export_csv_log,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_export_csv_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_export_csv_log,
            )
