from scania.mongo_db_operations.mongo_operations import mongodb_operations
from scania.s3_bucket_operations.blob_operation import blob_operation
from utils.logger import app_logger
from utils.read_params import read_params


class db_operation_pred:
    """
    Description :    This class shall be used for handling all the db operations

    Version     :    1.2
    Revisions   :    moved setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.pred_data_bucket = self.config["s3_bucket"]["scania_pred_data_bucket"]

        self.pred_export_csv_file = self.config["export_pred_csv_file"]

        self.good_data_pred_dir = self.config["data"]["pred"]["good_data_dir"]

        self.input_files_bucket = self.config["s3_bucket"]["input_files_bucket"]

        self.pred_db_insert_log = self.config["pred_db_log"]["db_insert"]

        self.pred_export_csv_log = self.config["pred_db_log"]["export_csv"]

        self.blob = blob_operation()

        self.db_op = mongodb_operations()

        self.log_writer = app_logger()

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
            lst = self.blob.read_csv(
                bucket=self.pred_data_bucket,
                file_name=self.good_data_pred_dir,
                collection_name=self.pred_db_insert_log,
                folder=True,
            )

            for idx, f in enumerate(lst):
                df = f[idx][1]

                file = f[idx][2]

                if file.endswith(".csv"):
                    self.db_op.insert_dataframe_as_record(
                        data_frame=df,
                        db_name=good_data_db_name,
                        collection_name=good_data_collection_name,
                        collection_name=self.pred_db_insert_log,
                    )

                else:
                    pass

                self.log_writer.log(
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
            df = self.db_op.get_collection_as_dataframe(
                db_name=good_data_db_name,
                collection_name=good_data_collection_name,
                collection_name=self.pred_export_csv_log,
            )

            self.blob.upload_df_as_csv(
                data_frame=df,
                file_name=self.pred_export_csv_file,
                bucket=self.input_files_bucket,
                dest_file_name=self.pred_export_csv_file,
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
