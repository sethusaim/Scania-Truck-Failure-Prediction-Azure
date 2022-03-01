from sklearn.ensemble import AdaBoostClassifier, RandomForestClassifier
from utils.logger import app_logger
from utils.model_utils import get_model_name, get_model_params, get_model_score
from utils.read_params import read_params


class model_finder:
    """
    Description :   This method is used for hyperparameter tuning of selected models
                    some preprocessing steps and then train the models and register them in mlflow
    Version     :   1.2
    Revisions   :   moved to setup to cloud
    """

    def __init__(self, table_name):
        self.table_name = table_name

        self.class_name = self.__class__.__name__

        self.config = read_params()

        self.log_writer = app_logger()

        self.cv = self.config["model_utils"]["cv"]

        self.verbose = self.config["model_utils"]["verbose"]

        self.n_jobs = self.config["model_utils"]["n_jobs"]

        self.ada_model = AdaBoostClassifier()

        self.rf_model = RandomForestClassifier()

    def get_best_model_for_adaboost(self, train_x, train_y):
        """
        Method Name :   get_best_params_for_xgb
        Description :   This method is used for getting the best params for xgboost model
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_best_model_for_adaboost.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=self.table_name,
        )

        try:
            self.ada_model_name = get_model_name(
                model=self.ada_model, table_name=self.table_name
            )

            self.adaboost_best_params = get_model_params(
                model=self.ada_model,
                model_key_name="adaboost_model",
                x_train=train_x,
                y_train=train_y,
                table_name=self.table_name,
            )

            self.n_estimators = self.adaboost_best_params["n_estimators"]

            self.learning_rate = self.adaboost_best_params["learning_rate"]

            self.random_state = self.adaboost_best_params["random_state"]

            self.log_writer.log(
                table_name=self.table_name,
                log_message=f"{self.ada_model_name} model best params are {self.adaboost_best_params}",
            )

            self.ada_model = AdaBoostClassifier(
                n_estimators=self.n_estimators,
                learning_rate=self.learning_rate,
                random_state=self.random_state,
            )

            self.log_writer.log(
                table_name=self.table_name,
                log_message=f"Initialized {self.ada_model_name} with {self.adaboost_best_params} as params",
            )

            self.ada_model.fit(train_x, train_y)

            self.log_writer.log(
                table_name=self.table_name,
                log_message=f"Created {self.ada_model_name} based on the {self.adaboost_best_params} as params",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=self.table_name,
            )

            return self.ada_model

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=self.table_name,
            )

    def get_best_model_for_rf(self, train_x, train_y):
        """
        Method Name :   get_best_params_for_xgb
        Description :   This method is used for getting the best params for xgboost model
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_best_model_for_rf.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=self.table_name,
        )

        try:
            self.rf_model_name = get_model_name(
                model=self.rf_model, table_name=self.table_name
            )

            self.rf_best_params = get_model_params(
                model=self.rf_model,
                model_key_name="rf_model",
                x_train=train_x,
                y_train=train_y,
                table_name=self.table_name,
            )

            self.criterion = self.rf_best_params["criterion"]

            self.max_depth = self.rf_best_params["max_depth"]

            self.max_features = self.rf_best_params["max_features"]

            self.n_estimators = self.rf_best_params["n_estimators"]

            self.log_writer.log(
                table_name=self.table_name,
                log_message=f"{self.rf_model_name} model best params are {self.rf_best_params}",
            )

            self.rf_model = RandomForestClassifier(
                n_estimators=self.n_estimators,
                criterion=self.criterion,
                max_depth=self.max_depth,
                max_features=self.max_features,
            )

            self.log_writer.log(
                table_name=self.table_name,
                log_message=f"Initialized {self.rf_model_name} with {self.rf_best_params} as params",
            )

            self.rf_model.fit(train_x, train_y)

            self.log_writer.log(
                table_name=self.table_name,
                log_message=f"Created {self.rf_model_name} based on the {self.rf_best_params} as params",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=self.table_name,
            )

            return self.rf_model

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=self.table_name,
            )

    def get_trained_models(self, train_x, train_y, test_x, test_y):
        """
        Method Name :   get_trained_models
        Description :   Find out the Model which has the best score.
        Output      :   The best model name and the model object
        On Failure  :   Raise Exception
        Written By  :   iNeuron Intelligence
        Version     :   1.0
        Revisions   :   None
        """
        method_name = self.get_trained_models.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            table_name=self.table_name,
        )

        try:
            ada_model = self.get_best_model_for_adaboost(
                train_x=train_x, train_y=train_y
            )

            ada_model_score = get_model_score(
                model=ada_model,
                test_x=test_x,
                test_y=test_y,
                table_name=self.table_name,
            )

            rf_model = self.get_best_model_for_rf(train_x=train_x, train_y=train_y)

            rf_model_score = get_model_score(
                model=rf_model,
                test_x=test_x,
                test_y=test_y,
                table_name=self.table_name,
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                table_name=self.table_name,
            )

            return (
                rf_model,
                rf_model_score,
                ada_model,
                ada_model_score,
            )

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                table_name=self.table_name,
            )
