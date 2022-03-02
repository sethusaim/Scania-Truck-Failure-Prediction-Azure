import pandas as pd
from scania.blob_storage_operations.blob_operations import Blob_Operation
from scania.data_ingestion.data_loader_prediction import Data_Getter_Pred
from scania.data_preprocessing.preprocessing import Preprocessor
from utils.logger import App_Logger
from utils.read_params import read_params


class Prediction:
    """
    Description :   This class shall be used for loading the production model

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self):
        self.config = read_params()

        self.db_name = self.config["db_log"]["pred"]

        self.pred_log = self.config["pred_db_log"]["pred_main"]

        self.model_container = self.config["container"]["phising_model"]

        self.input_files_container = self.config["container"]["inputs_files"]

        self.prod_model_dir = self.config["models_dir"]["prod"]

        self.pred_output_file = self.config["pred_output_file"]

        self.log_writer = App_Logger()

        self.blob = Blob_Operation()

        self.data_getter_pred = Data_Getter_Pred(
            db_name=self.db_name, collection_name=self.pred_log
        )

        self.preprocessor = Preprocessor(
            db_name=self.db_name, collection_name=self.pred_log
        )

        self.class_name = self.__class__.__name__

    def delete_pred_file(self):
        """
        Method Name :   delete_pred_file
        Description :   This method is used for deleting the existing prediction batch file

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.delete_pred_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_log,
        )

        try:
            f = self.blob.load_file(
                file_name=self.pred_output_file,
                container_name=self.input_files_container,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

            if f is True:
                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                    log_info="Found existing prediction batch file. Deleting it.",
                )

                self.blob.delete_file(
                    file_name=self.pred_output_file,
                    container_name=self.input_files_container,
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                )

            else:
                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                    log_info="Previous prediction file is not found, not deleting it",
                )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

    def find_correct_model_file(self, cluster_number, container_name):
        """
        Method Name :   find_correct_model_file
        Description :   This method is used for finding the correct model file during prediction

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.find_correct_model_file.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_log,
        )

        try:
            list_of_files = self.blob.get_files_from_folder(
                folder_name=self.prod_model_dir,
                container_name=container_name,
                db_name=self.db_name,
                collection_name=container_name,
            )

            for file in list_of_files:
                try:
                    if file.index(str(cluster_number)) != -1:
                        model_name = file

                except:
                    continue

            model_name = model_name.split(".")[0]

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.pred_log,
                log_info=f"Got {model_name} from {self.prod_model_dir} folder in {container_name} container",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

            return model_name

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

    def predict_from_model(self):
        """
        Method Name :   predict_from_model
        Description :   This method is used for loading from prod model dir of blob container and use them for prediction

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.predict_from_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.pred_log,
        )

        try:
            self.delete_pred_file()

            data = self.data_getter_pred.get_data()

            is_null_present = self.preprocessor.is_null_present(data)

            if is_null_present:
                data = self.preprocessor.impute_missing_values(data)

            cols_to_drop = self.preprocessor.get_columns_with_zero_std_deviation(data)

            data = self.preprocessor.remove_columns(data, cols_to_drop)

            kmeans = self.blob.load_model(
                model_name="KMeans",
                container_name=self.model_container,
                model_dir=self.prod_model_dir,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )

            clusters = kmeans.predict(data.drop(["scania"], axis=1))

            data["clusters"] = clusters

            clusters = data["clusters"].unique()

            for i in clusters:
                cluster_data = data[data["clusters"] == i]

                phising_names = list(cluster_data["scania"])

                cluster_data = data.drop(labels=["scania"], axis=1)

                cluster_data = cluster_data.drop(["clusters"], axis=1)

                crt_model_name = self.find_correct_model_file(
                    cluster_number=i,
                    container_name=self.model_container,
                )

                model = self.blob.load_model(
                    model_name=crt_model_name,
                    container_name=self.model_container,
                    model_dir=self.prod_model_dir,
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                )

                result = list(model.predict(cluster_data))

                result = pd.DataFrame(
                    list(zip(phising_names, result)), columns=["scania", "prediction"]
                )

                self.blob.upload_df_as_csv(
                    dataframe=result,
                    local_file_name=self.pred_output_file,
                    container_file_name=self.pred_output_file,
                    container_name=self.input_files_container,
                    db_name=self.db_name,
                    collection_name=self.pred_log,
                )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.pred_log,
                log_info=f"End of Prediction",
            )

            return (
                self.input_files_container,
                self.pred_output_file,
                result.head().to_json(orient="records"),
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.pred_log,
            )
