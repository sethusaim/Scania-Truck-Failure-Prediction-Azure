from scania.container_operations.blob_operation import blob_operation
from utils.logger import app_logger
from utils.read_params import read_params


class data_transform_train:
    """
    Description : This class shall be used for transforming the trainiction batch data before loading it in Database!!.

    Version     : 1.2
    Revisions   : moved setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.train_data_container = self.config["container"][
            "scania_train_data_container"
        ]

        self.blob = blob_operation()

        self.log_writer = app_logger()

        self.good_train_data_dir = self.config["data"]["train"]["good_data_dir"]

        self.class_name = self.__class__.__name__

        self.train_data_transform_log = self.config["train_db_log"]["data_transform"]

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
            collection_name=self.train_data_transform_log,
        )

        try:
            lst = self.blob.read_csv(
                container=self.train_data_container,
                file_name=self.good_train_data_dir,
                collection_name=self.train_data_transform_log,
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
                    collection_name=self.train_data_transform_log,
                    log_info=f"Quotes added for the file {file}",
                )

                self.blob.upload_df_as_csv(
                    data_frame=df,
                    file_name=abs_f,
                    container=self.train_data_container,
                    dest_file_name=file,
                    collection_name=self.train_data_transform_log,
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_data_transform_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                collection_name=self.train_data_transform_log,
            )
