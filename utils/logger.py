from datetime import datetime
from scania.mongo_db_operations.mongo_operations import MongoDB_Operation


class App_Logger:
    def __init__(self):
        self.mongo = MongoDB_Operation()

        self.class_name = self.__class__.__name__

    def log(self, db_name, collection_name, log_info):
        try:
            log = {
                "Log_updated_date": str(datetime.now()),
                "Log_updated_time": str(datetime.now().strftime("%H:%M:%S")),
                "Log_Info": log_info,
            }

            self.mongo.insert_record(
                db_name=db_name, collection_name=collection_name, data=log
            )

        except Exception as e:
            raise e

    def start_log(self, key, class_name, method_name, db_name, collection_name):
        """
        Method Name :   start_log
        Description :   This method is used for logging the entry or exit of method depending on key value
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """
        start_method_name = self.start_log.__name__

        try:
            func = lambda: "Entered" if key == "start" else "Exited"

            key = func()

            log_msg = f"{key} {method_name} method of class {class_name}"

            self.log(db_name=db_name, collection_name=collection_name, log_info=log_msg)

        except Exception as e:
            error_msg = f"Exception occured in Class : {self.class_name}, Method : {start_method_name}, Error : {str(e)}"

            raise Exception(error_msg)

    def exception_log(self, error, class_name, method_name, db_name, collection_name):
        """
        Method Name :   exception_log
        Description :   This method is used for logging exception
        Version     :   1.2
        Revisions   :   moved setup to cloud
        """

        self.start_log(
            key="exit",
            class_name=class_name,
            method_name=method_name,
            db_name=db_name,
            collection_name=collection_name,
        )

        exception_msg = f"Exception occured in Class : {class_name}, Method : {method_name}, Error : {str(error)}"

        self.log(
            db_name=db_name, collection_name=collection_name, log_info=exception_msg
        )

        raise Exception(exception_msg)
