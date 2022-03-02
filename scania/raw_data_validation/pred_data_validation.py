import re

from scania.blob_storage_operations.blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.read_params import read_params


class Raw_Pred_Data_Validation:
    """
    Description :   This method is used for validating the raw prediction data

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self, raw_data_container_name):
        self.config = read_params()

        self.db_name = self.config["db_log"]["pred"]

        self.raw_data_container_name = raw_data_container_name

        self.log_writer = App_Logger()

        self.class_name = self.__class__.__name__

        self.blob = Blob_Operation()

        self.pred_data_container = self.config["container"]["pred_data"]

        self.input_files = self.config["container"]["input_files"]

        self.raw_pred_data_dir = self.config["data"]["raw_data"]["pred"]

        self.pred = self.config["schema_file"]["pred"]

        self.regex_file = self.config["regex_file"]

        self.pred_schema_log = self.config["pred_db_log"]["values_from_schema"]

        self.good_pred_data_dir = self.config["data"]["pred"]["good"]

        self.bad_pred_data_dir = self.config["data"]["pred"]["bad"]

        self.pred_gen_log = self.config["pred_db_log"]["general"]

        self.pred_name_valid_log = self.config["pred_db_log"]["name_validation"]

        self.pred_col_valid_log = self.config["pred_db_log"]["col_validation"]

        self.pred_missing_value_log = self.config["pred_db_log"][
            "missing_values_in_col"
        ]

    def values_from_schema(self):
        """
        Method Name :   values_from_schema
        Description :   This method is used for getting values from schema_prediction.json

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.values_from_schema.__name__

        try:
            self.log_writer.start_log(
                key="start",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_schema_log,
            )

            dic = self.blob.read_json(
                db_name=self.db_name,
                collection_name=self.pred_schema_log,
                container_name=self.input_files,
                file_name=self.pred,
            )

            LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]

            LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]

            column_names = dic["ColName"]

            NumberofColumns = dic["NumberofColumns"]

            message = (
                "LengthOfDateStampInFile: %s" % LengthOfDateStampInFile
                + "\t"
                + "LengthOfTimeStampInFile: %s" % LengthOfTimeStampInFile
                + "\t "
                + "NumberofColumns: %s" % NumberofColumns
                + "\n"
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.pred_schema_log,
                log_info=message,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_schema_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_schema_log,
            )

        return (
            LengthOfDateStampInFile,
            LengthOfTimeStampInFile,
            column_names,
            NumberofColumns,
        )

    def get_regex_pattern(self):
        """
        Method Name :   get_regex_pattern
        Description :   This method is used for getting regex pattern for file validation

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_regex_pattern.__name__

        try:
            self.log_writer.start_log(
                key="start",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_gen_log,
            )

            regex = self.blob.read_text(
                file_name=self.regex_file,
                container_name=self.input_files,
                db_name=self.db_name,
                collection_name=self.pred_gen_log,
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.pred_gen_log,
                log_info=f"Got {regex} pattern",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_gen_log,
            )

            return regex

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_gen_log,
            )

    def validate_raw_file_name(
        self, regex, LengthOfDateStampInFile, LengthOfTimeStampInFile
    ):
        """
        Method Name :   validate_raw_file_name
        Description :   This method is used for validating raw file name based on the regex pattern

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.validate_raw_file_name.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_name_valid_log,
        )

        try:
            onlyfiles = self.blob.get_files_from_folder(
                db_name=self.db_name,
                collection_name=self.pred_name_valid_log,
                container_name=self.raw_data_container_name,
                folder_name=self.raw_pred_data_dir,
            )

            pred_batch_files = [f.split("/")[1] for f in onlyfiles]

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.pred_name_valid_log,
                log_info="Got prediction files with exact name",
            )

            for filename in pred_batch_files:
                raw_data_pred_filename = self.raw_pred_data_dir + "/" + filename

                good_data_pred_filename = self.good_pred_data_dir + "/" + filename

                bad_data_pred_filename = self.bad_pred_data_dir + "/" + filename

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.pred_name_valid_log,
                    log_info="Created raw,good and bad data filenames",
                )

                if re.match(regex, filename):
                    splitAtDot = re.split(".csv", filename)

                    splitAtDot = re.split("_", splitAtDot[0])

                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            self.blob.copy_data(
                                from_file_name=raw_data_pred_filename,
                                from_container_name=self.raw_data_container_name,
                                to_file_name=good_data_pred_filename,
                                to_container_name=self.pred_data_container,
                                db_name=self.db_name,
                                collection_name=self.pred_name_valid_log,
                            )

                        else:
                            self.blob.copy_data(
                                from_file_name=raw_data_pred_filename,
                                from_container_name=self.raw_data_container_name,
                                to_file_name=bad_data_pred_filename,
                                to_container_name=self.pred_data_container,
                                db_name=self.db_name,
                                collection_name=self.pred_name_valid_log,
                            )

                    else:
                        self.blob.copy_data(
                            from_file_name=raw_data_pred_filename,
                            from_container_name=self.raw_data_container_name,
                            to_file_name=bad_data_pred_filename,
                            to_container_name=self.pred_data_container,
                            db_name=self.db_name,
                            collection_name=self.pred_name_valid_log,
                        )

                else:
                    self.blob.copy_data(
                        from_file_name=raw_data_pred_filename,
                        from_container_name=self.raw_data_container_name,
                        to_file_name=bad_data_pred_filename,
                        to_container_name=self.pred_data_container,
                        db_name=self.db_name,
                        collection_name=self.pred_name_valid_log,
                    )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_name_valid_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_name_valid_log,
            )

    def validate_col_length(self, NumberofColumns):
        """
        Method Name :   validate_col_length
        Description :   This method is used for validating the column length of the csv file

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.validate_col_length.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_col_valid_log,
        )

        try:
            lst = self.blob.read_csv_from_folder(
                folder_name=self.good_pred_data_dir,
                container_name=self.pred_data_container,
                db_name=self.db_name,
                collection_name=self.pred_col_valid_log,
            )

            for f in lst:
                df = f[0]

                file = f[1]

                abs_f = f[2]

                if file.endswith(".csv"):
                    if df.shape[1] == NumberofColumns:
                        pass

                    else:
                        dest_f = self.bad_pred_data_dir + "/" + abs_f

                        self.blob.move_data(
                            from_file_name=file,
                            from_container_name=self.pred_data_container,
                            to_file_name=dest_f,
                            to_container_name=self.pred_data_container,
                            db_name=self.db_name,
                            collection_name=self.pred_col_valid_log,
                        )

                else:
                    pass

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_col_valid_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_col_valid_log,
            )

    def validate_missing_values_in_col(self):
        """
        Method Name :   validate_missing_values_in_col
        Description :   This method is used for validating the missing values in columns

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.validate_missing_values_in_col.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_missing_value_log,
        )

        try:
            lst = self.blob.read_csv_from_folder(
                folder_name=self.good_pred_data_dir,
                container_name=self.pred_data_container,
                db_name=self.db_name,
                collection_name=self.pred_missing_value_log,
            )

            for f in lst:
                df = f[0]

                file = f[1]

                abs_f = f[2]

                if abs_f.endswith(".csv"):
                    count = 0

                    for cols in df:
                        if (len(df[cols]) - df[cols].count()) == len(df[cols]):
                            count += 1

                            dest_f = self.bad_pred_data_dir + "/" + abs_f

                            self.blob.move_data(
                                from_file_name=file,
                                from_container_name=self.pred_data_container,
                                to_file_name=dest_f,
                                to_container_name=self.pred_data_container,
                                db_name=self.db_name,
                                collection_name=self.pred_missing_value_log,
                            )

                            break

                    if count == 0:
                        dest_f = self.good_pred_data_dir + "/" + abs_f

                        self.blob.upload_df_as_csv(
                            dataframe=df,
                            local_file_name=abs_f,
                            container_file_name=dest_f,
                            container_name=self.pred_data_container,
                            db_name=self.db_name,
                            collection_name=self.pred_missing_value_log,
                        )

                else:
                    pass

                self.log_writer.start_log(
                    key="exit",
                    class_name=self.class_name,
                    method_name=method_name,
                    db_name=self.db_name,
                    collection_name=self.pred_missing_value_log,
                )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_missing_value_log,
            )
