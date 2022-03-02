from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.model_selection import GridSearchCV

from utils.logger import App_Logger
from utils.read_params import read_params


class Model_Utils:
    def __init__(self, db_name, collection_name):
        self.log_writer = App_Logger()

        self.config = read_params()

        self.db_name = db_name

        self.collection_name = collection_name

        self.class_name = self.__class__.__name__

    def get_model_name(self, model):
        """
        Method Name :   get_model_name
        Description :   This method is used for getting the actual model name

        Version     :   1.2
        Revisions   :   Moved to setup to cloud
        """
        method_name = self.get_model_name.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            model_name = model.__class__.__name__

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Got the {model} model_name",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return model_name

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_model_param_grid(self, model_key_name):
        """
        Method Name :   get_model_param_grid
        Description :   This method is used for getting the param dict from params.yaml file

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_model_param_grid.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            model_grid = {}

            model_grid.__getattribute__

            model_param_name = self.config["model_params"][model_key_name]

            params_names = list(model_param_name.keys())

            for param in params_names:
                model_grid[param] = model_param_name[param]

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Inserted {model_key_name} params to model_grid dict",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return model_grid

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_model_score(self, model, test_x, test_y):
        """
        Method Name :   get_model_score
        Description :   This method is used for calculating the best score for the model based on the test data

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_model_score.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            model_name = self.get_model_name(
                model=model, db_name=self.db_name, collection_name=self.collection_name
            )

            preds = model.predict(test_x)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Used {model_name} model to get predictions on test data",
            )

            if len(test_y.unique()) == 1:
                model_score = accuracy_score(test_y, preds)

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                    log_info=f"Accuracy for {model_name} is {model_score}",
                )

            else:
                model_score = roc_auc_score(test_y, preds)

                self.log_writer.log(
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                    log_info=f"AUC score for {model_name} is {model_score}",
                )

                self.log_writer.start_log(
                    key="exit",
                    class_name=self.class_name,
                    method_name=method_name,
                    db_name=self.db_name,
                    collection_name=self.collection_name,
                )

            return model_score

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

    def get_model_params(self, model, model_key_name, x_train, y_train):
        """
        Method Name :   get_model_params
        Description :   This method is used for finding the best params for the given model

        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        method_name = self.get_model_params.__name__

        self.log_writer.start_log(
            key="start",
            class_name=self.class_name,
            method_name=method_name,
            db_name=self.db_name,
            collection_name=self.collection_name,
        )

        try:
            cv = self.config["model_utils"]["cv"]

            verbose = self.config["model_utils"]["verbose"]

            n_jobs = self.config["model_utils"]["n_jobs"]

            model_name = self.get_model_name(model=model)

            model_param_grid = self.get_model_param_grid(model_key_name=model_key_name)

            model_grid = GridSearchCV(
                estimator=model,
                param_grid=model_param_grid,
                cv=cv,
                verbose=verbose,
                n_jobs=n_jobs,
            )

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Initialized {model_grid.__class__.__name__}  with {model_param_grid} as params",
            )

            model_grid.fit(x_train, y_train)

            self.log_writer.log(
                db_name=self.db_name,
                collection_name=self.collection_name,
                log_info=f"Found the best params for {model_name} model based on {model_param_grid} as params",
            )

            self.log_writer.start_log(
                key="exit",
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )

            return model_grid.best_params_

        except Exception as e:
            self.log_writer.exception_log(
                error=e,
                class_name=self.class_name,
                method_name=method_name,
                db_name=self.db_name,
                collection_name=self.collection_name,
            )
