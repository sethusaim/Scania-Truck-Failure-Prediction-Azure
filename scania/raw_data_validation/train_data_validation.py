import re

from scania.s3_bucket_operations.blob_operation import blob_operation
from utils.logger import app_logger
from utils.read_params import read_params


class raw_train_data_validation:
    """
    Description :   This method is used for validating the raw training data

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self, raw_data_bucket_name):
        self.config = read_params()

        self.raw_data_bucket_name = raw_data_bucket_name

        self.log_writer = app_logger()

        self.class_name = self.__class__.__name__

        self.blob = blob_operation()

        self.train_data_bucket = self.config["s3_bucket"]["scania_train_data_bucket"]

        self.input_files_bucket = self.config["s3_bucket"]["input_files_bucket"]

        self.raw_train_data_dir = self.config["data"]["raw_data"]["train_batch"]

        self.train_schema_file = self.config["schema_file"]["train_schema_file"]

        self.scania_regex_file = self.config["regex_file"]

        self.train_schema_log = self.config["train_db_log"]["values_from_schema"]

        self.good_train_data_dir = self.config["data"]["train"]["good_data_dir"]

        self.bad_train_data_dir = self.config["data"]["train"]["bad_data_dir"]

        self.train_gen_log = self.config["train_db_log"]["general"]

        self.train_name_valid_log = self.config["train_db_log"]["name_validation"]

        self.train_col_valid_log = self.config["train_db_log"]["col_validation"]

        self.train_missing_value_log = self.config["train_db_log"][
            "missing_values_in_col"
        ]

    def values_from_schema(self):
        """
        Method Name :   values_from_schema
        Description :   This method is used for getting values from schema_trainiction.json

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.values_from_schema.__name__

        try:
            self.log_writer.start_log(
                key="start",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_schema_log,
            )

            dic = self.blob.read_json(
                bucket=self.input_files_bucket,
                filename=self.train_schema_file,
                collection_name=self.train_schema_log,
            )

            LengthOfDateStampInFile = dic["LengthOfDateStampInFile"]

            LengthOfTimeStampInFile = dic["LengthOfTimeStampInFile"]

            column_names = dic["ColName"]

            NumberofColumns = dic["NumberofColumns"]

            message = (
                "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile
                + "\t"
                + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile
                + "\t "
                + "NumberofColumns:: %s" % NumberofColumns
                + "\n"
            )

            self.log_writer.log(
                collection_name=self.train_schema_log,
                log_info=message,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_schema_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_schema_log,
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
                collection_name=self.train_gen_log,
            )

            regex = self.blob.read_text(
                file_name=self.scania_regex_file,
                bucket_name=self.input_files_bucket,
                collection_name=self.train_gen_log,
            )

            self.log_writer.log(
                collection_name=self.train_gen_log,
                log_info=f"Got {regex} pattern",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_gen_log,
            )

            return regex

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_gen_log,
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
            collection_name=self.train_name_valid_log,
        )

        try:
            self.blob.create_dirs_for_good_bad_data(
                collection_name=self.train_name_valid_log
            )

            onlyfiles = self.blob.get_files(
                bucket=self.raw_data_bucket_name,
                folder_name=self.raw_train_data_dir,
                collection_name=self.train_name_valid_log,
            )

            train_batch_files = [f.split("/")[1] for f in onlyfiles]

            self.log_writer.log(
                collection_name=self.train_name_valid_log,
                log_info="Got trainiction files with exact name",
            )

            for filename in train_batch_files:
                raw_data_train_filename = self.raw_train_data_dir + "/" + filename

                good_data_train_filename = self.good_train_data_dir + "/" + filename

                bad_data_train_filename = self.bad_train_data_dir + "/" + filename

                self.log_writer.log(
                    collection_name=self.train_name_valid_log,
                    log_info="Created raw,good and bad data filenames",
                )

                if re.match(regex, filename):
                    splitAtDot = re.split(".csv", filename)

                    splitAtDot = re.split("_", splitAtDot[0])

                    if len(splitAtDot[1]) == LengthOfDateStampInFile:
                        if len(splitAtDot[2]) == LengthOfTimeStampInFile:
                            self.blob.copy_data(
                                src_bucket=self.raw_data_bucket_name,
                                src_file=raw_data_train_filename,
                                dest_bucket=self.train_data_bucket,
                                dest_file=good_data_train_filename,
                                collection_name=self.train_name_valid_log,
                            )

                        else:
                            self.blob.copy_data(
                                src_bucket=self.raw_data_bucket_name,
                                src_file=raw_data_train_filename,
                                dest_bucket=self.train_data_bucket,
                                dest_file=bad_data_train_filename,
                                collection_name=self.train_name_valid_log,
                            )

                    else:
                        self.blob.copy_data(
                            src_bucket=self.raw_data_bucket_name,
                            src_file=raw_data_train_filename,
                            dest_bucket=self.train_data_bucket,
                            dest_file=bad_data_train_filename,
                            collection_name=self.train_name_valid_log,
                        )

                else:
                    self.blob.copy_data(
                        src_bucket=self.raw_data_bucket_name,
                        src_file=raw_data_train_filename,
                        dest_bucket=self.train_data_bucket,
                        dest_file=bad_data_train_filename,
                        collection_name=self.train_name_valid_log,
                    )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_name_valid_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_name_valid_log,
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
            collection_name=self.train_col_valid_log,
        )

        try:
            lst = self.blob.read_csv(
                bucket=self.train_data_bucket,
                file_name=self.good_train_data_dir,
                folder=True,
                collection_name=self.train_col_valid_log,
            )

            for idx, f in enumerate(lst):
                df = f[idx][0]

                file = f[idx][1]

                abs_f = f[idx][2]

                if file.endswith(".csv"):
                    if df.shape[1] == NumberofColumns:
                        pass

                    else:
                        dest_f = self.bad_train_data_dir + "/" + abs_f

                        self.blob.move_data(
                            src_bucket=self.train_data_bucket,
                            src_file=file,
                            dest_bucket=self.train_data_bucket,
                            dest_file=dest_f,
                            collection_name=self.train_col_valid_log,
                        )

                else:
                    pass

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_col_valid_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_col_valid_log,
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
            collection_name=self.train_missing_value_log,
        )

        try:
            lst = self.blob.read_csv(
                bucket=self.train_data_bucket,
                file_name=self.good_train_data_dir,
                folder=True,
                collection_name=self.train_missing_value_log,
            )

            for idx, f in enumerate(lst):
                df = f[idx][0]

                file = f[idx][1]

                abs_f = f[idx][2]

                if abs_f.endswith(".csv"):
                    count = 0

                    for cols in df:
                        if (len(df[cols]) - df[cols].count()) == len(df[cols]):
                            count += 1

                            dest_f = self.bad_train_data_dir + "/" + abs_f

                            self.blob.move_data(
                                src_bucket=self.train_data_bucket,
                                src_file=file,
                                dest_bucket=self.train_data_bucket,
                                dest_file=dest_f,
                                collection_name=self.train_missing_value_log,
                            )

                            break

                    if count == 0:
                        dest_f = self.good_train_data_dir + "/" + abs_f

                        self.blob.upload_df_as_csv(
                            data_frame=df,
                            file_name=abs_f,
                            bucket=self.train_data_bucket,
                            dest_file_name=dest_f,
                            collection_name=self.train_missing_value_log,
                        )

                else:
                    pass

                self.log_writer.start_log(
                    key="exit",
                    class_name=self.class_name,
                    method_name=method_name,
                    collection_name=self.train_missing_value_log,
                )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_missing_value_log,
            )
