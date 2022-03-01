from scania.blob_storage_operations.blob_operations import Blob_Operation
from utils.logger import App_Logger
from utils.read_params import read_params


class data_transform_pred:
    """
    Description : This class shall be used for transforming the Prediction batch data before loading it in Database!!.

    Version     : 1.2
    Revisions   : moved setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.pred_data_bucket = self.config["s3_bucket"]["scania_pred_data_bucket"]

        self.blob = Blob_Operation()

        self.log_writer = App_Logger()

        self.good_pred_data_dir = self.config["data"]["pred"]["good_data_dir"]

        self.class_name = self.__class__.__name__

        self.pred_data_transform_log = self.config["pred_db_log"]["data_transform"]

    def add_quotes_to_string(self):
        """
        Method Name :   add_quotes_to_string
        Description :   This method addes the quotes to the string data present in columns

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.add_quotes_to_string.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            collection_name=self.pred_data_transform_log,
        )

        try:
            lst = self.blob.read_csv(
                bucket=self.pred_data_bucket,
                file_name=self.good_pred_data_dir,
                collection_name=self.pred_data_transform_log,
                folder=True,
            )

            for idx, t_pdf in enumerate(lst):
                df = t_pdf[idx][1]

                file = t_pdf[idx][2]

                abs_f = t_pdf[idx][3]

                df["class"] = df["class"].apply(lambda x: "'" + str(x) + "'")

                for column in df.columns:
                    count = df[column][df[column] == "na"].count()

                    if count != 0:
                        df[column] = df[column].replace("na", "'na'")

                self.log_writer.log(
                    collection_name=self.pred_data_transform_log,
                    log_info=f"Quotes added for the file {file}",
                )

                self.blob.upload_df_as_csv(
                    data_frame=df,
                    file_name=abs_f,
                    bucket=self.pred_data_bucket,
                    dest_file_name=file,
                    collection_name=self.pred_data_transform_log,
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_data_transform_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.pred_data_transform_log,
            )
