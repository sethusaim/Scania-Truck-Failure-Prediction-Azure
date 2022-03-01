from scania.data_transform.data_transformation_pred import data_transform_pred
from scania.data_type_valid.data_type_valid_pred import db_operation_pred
from scania.raw_data_validation.pred_data_validation import raw_pred_data_validation
from utils.logger import app_logger
from utils.read_params import read_params


class Pred_Validation:
    """
    Description :   This class is used for validating all the Prediction batch files

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self, container_name):
        self.raw_data = raw_pred_data_validation(raw_data_container_name=container_name)

        self.data_transform = data_transform_pred()

        self.db_operation = db_operation_pred()

        self.config = read_params()

        self.class_name = self.__class__.__name__

        self.pred_main_log = self.config["pred_db_log"]["pred_main"]

        self.good_data_db_name = self.config["mongodb"]["scania_data_db_name"]

        self.good_data_collection_name = self.config["mongodb"][
            "scania_pred_data_collection"
        ]

        self.log_writer = app_logger()

    def Prediction_validation(self):
        """
        Method Name :   load_blob_obj
        Description :   This method is used for validating the Prediction btach files

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.Prediction_validation.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            collection_name=self.pred_main_log,
        )

        try:
            (
                LengthOfDateStampInFile,
                LengthOfTimeStampInFile,
                column_names,
                noofcolumns,
            ) = self.raw_data.values_from_schema()

            regex = self.raw_data.get_regex_pattern()

            self.raw_data.validate_raw_file_name(
                regex, LengthOfDateStampInFile, LengthOfTimeStampInFile
            )

            self.raw_data.validate_col_length(NumberofColumns=noofcolumns)

            self.raw_data.validate_missing_values_in_col()

            self.log_writer.log(
                collection_name=self.pred_main_log,
                log_info="Raw Data Validation Completed !!",
            )

            self.log_writer.log(
                collection_name=self.pred_main_log,
                log_info="Starting Data Transformation",
            )

            self.data_transform.add_quotes_to_string()

            self.log_writer.log(
                collection_name=self.pred_main_log,
                log_info="Data Transformation completed !!",
            )

            self.db_operation.insert_good_data_as_record(
                db_name=self.good_data_db_name,
                collection_name=self.good_data_collection_name,
            )

            self.log_writer.log(
                collection_name=self.pred_main_log,
                log_info="Data type validation Operation completed !!",
            )

            self.db_operation.export_collection_to_csv(
                db_name=self.good_data_db_name,
                collection_name=self.good_data_collection_name,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_main_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_main_log,
            )
