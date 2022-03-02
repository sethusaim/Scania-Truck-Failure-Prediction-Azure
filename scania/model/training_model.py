import mlflow
from scania.blob_storage_operations.blob_operations import Blob_Operation
from scania.data_ingestion.data_loader_train import Data_Getter_Train
from scania.data_preprocessing.clustering import KMeans_Clustering
from scania.data_preprocessing.preprocessing import Preprocessor
from scania.mlflow_utils.mlflow_operations import MLFlow_Operation
from scania.model_finder.tuner import Model_Finder
from sklearn.model_selection import train_test_split
from utils.logger import App_Logger
from utils.read_params import read_params


class Train_Model:
    """
    Description :   This method is used for getting the data and applying
                    some preprocessing steps and then train the models and register them in mlflow

    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self):
        self.log_writer = App_Logger()

        self.config = read_params()

        self.db_name = self.config["db_log"]["train"]

        self.model_train_log = self.config["train_db_log"]["train_model"]

        self.model_container = self.config["container"]["phising_model"]

        self.test_size = self.config["base"]["test_size"]

        self.target_col = self.config["base"]["target_col"]

        self.random_state = self.config["base"]["random_state"]

        self.remote_server_uri = self.config["mlflow_config"]["remote_server_uri"]

        self.experiment_name = self.config["mlflow_config"]["experiment_name"]

        self.run_name = self.config["mlflow_config"]["run_name"]

        self.train_model_dir = self.config["models_dir"]["trained"]

        self.class_name = self.__class__.__name__

        self.mlflow_op = MLFlow_Operation(
            db_name=self.db_name, collection_name=self.model_train_log
        )

        self.data_getter_train = Data_Getter_Train(
            db_name=self.db_name, collection_name=self.model_train_log
        )

        self.preprocessor = Preprocessor(
            db_name=self.db_name, collection_name=self.model_train_log
        )

        self.kmeans_op = KMeans_Clustering(
            db_name=self.db_name, collection_name=self.model_train_log
        )

        self.Model_Finder_obj = Model_Finder(
            db_name=self.db_name, collection_name=self.model_train_log
        )

        self.blob = Blob_Operation()

    def training_model(self):
        """
        Method Name :   training_model
        Description :   This method is used for getting the data and applying
                        some preprocessing steps and then train the models and register them in mlflow

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.training_model.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.model_train_log,
        )

        try:
            df = self.data_getter_train.get_data()

            df = self.preprocessor.replace_invalid_values(data=df)

            df = self.preprocessor.encode_target_cols(data=df)

            is_null_present = self.preprocessor.is_null_present(data=df)

            if is_null_present:
                df = self.preprocessor.impute_missing_values(data=df)

            X, Y = self.preprocessor.separate_label_feature(
                data=df, label_column_name=self.target_col
            )

            cols_to_drop = self.preprocessor.get_columns_with_zero_std_deviation(
                data=df
            )

            X = self.preprocessor.remove_columns(data=df, columns=cols_to_drop)

            X = self.preprocessor.scale_numerical_columns(data=df)

            X = self.preprocessor.apply_pca_transform(X_scaled_data=X)

            number_of_clusters = self.kmeans_op.elbow_plot(X)

            X, kmeans_model = self.kmeans_op.create_clusters(
                data=X, number_of_clusters=number_of_clusters
            )

            X["Labels"] = Y

            list_of_clusters = X["Cluster"].unique()

            self.log_writer.log(
                table_name=self.model_train_log,
                log_info="Got unique list of clusters",
            )

            for i in list_of_clusters:
                cluster_data = X[X["Cluster"] == i]

                cluster_features = cluster_data.drop(["Labels", "Cluster"], axis=1)

                cluster_label = cluster_data["Labels"]

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.model_train_log,
                    log_info="Seprated cluster features and cluster label for the cluster data",
                )

                x_train, x_test, y_train, y_test = train_test_split(
                    cluster_features,
                    cluster_label,
                    test_size=self.test_size,
                    random_state=self.random_state,
                )

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.model_train_log,
                    log_info=f"Performed train test split with test size as {self.test_size} and random state as {self.random_state}",
                )

                (
                    xgb_model,
                    xgb_model_score,
                    rf_model,
                    rf_model_score,
                ) = self.Model_Finder_obj.get_trained_models(
                    x_train, y_train, x_test, y_test
                )

                self.blob.save_model(
                    model=xgb_model,
                    model_dir=self.train_model_dir,
                    container_name=self.model_container,
                    idx=i,
                    db_name=self.db_name,
                    collection_name=self.model_train_log,
                )

                self.blob.save_model(
                    model=rf_model,
                    idx=i,
                    model_dir=self.train_model_dir,
                    model_container=self.model_container,
                    db_name=self.db_name,
                    collection_name=self.model_train_log,
                )

                try:
                    self.mlflow_op.set_mlflow_tracking_uri(
                        server_uri=self.remote_server_uri
                    )

                    self.mlflow_op.set_mlflow_experiment(
                        experiment_name=self.experiment_name
                    )

                    with mlflow.start_run(run_name=self.run_name):
                        self.mlflow_op.log_all_for_model(
                            idx=None,
                            model=kmeans_model,
                            model_param_name=None,
                            model_score=None,
                        )

                        self.mlflow_op.log_all_for_model(
                            idx=i,
                            model=xgb_model,
                            model_param_name="xgb_model",
                            model_score=xgb_model_score,
                        )

                        self.mlflow_op.log_all_for_model(
                            idx=i,
                            model=rf_model,
                            model_param_name="rf_model",
                            model_score=rf_model_score,
                        )

                except Exception as e:
                    self.log_writer.log(
                        db_name=self.db_name,
                        collection_name=self.model_train_log,
                        log_info="Mlflow logging of params,metrics and models failed",
                    )

                    self.log_writer.exception_log(
                        error=e,
                        class_name=self.class_name,
                        method_name=method_name,
                        db_name=self.db_name,
                        collection_name=self.model_train_log,
                    )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.model_train_log,
                log_info="Successful End of Training",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.model_train_log,
            )

            return number_of_clusters

        except Exception as e:
            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.model_train_log,
                log_info="Unsuccessful End of Training",
            )

            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.model_train_log,
            )
