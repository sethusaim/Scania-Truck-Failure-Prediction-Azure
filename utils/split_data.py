import csv
import os
import time
from datetime import datetime


def split_csv(source_filepath, dest_folder, records_per_file):
    if records_per_file <= 0:
        raise Exception("records_per_file must be > 0")

    with open(source_filepath, "r") as source:
        reader = csv.reader(source)

        headers = next(reader)

        file_idx = 0

        records_exist = True

        while records_exist:
            i = 0

            time.sleep(10)

            now = datetime.now()

            now_date = now.date()

            now_time = now.time().strftime("%H:%M:%S")

            date_lst = str(now_date).split("-")

            dt_stamp = ""

            tt_stamp = ""

            datetimestamp = dt_stamp.join(date_lst)

            time_lst = str(now_time).split(":")

            timestamp = tt_stamp.join(time_lst)

            file_format = f"apsfailure_{datetimestamp}_{timestamp}"

            target_filename = f"{file_format}.csv"

            target_filepath = os.path.join(dest_folder, target_filename)

            with open(target_filepath, "w") as target:
                writer = csv.writer(target)

                while i < records_per_file:
                    if i == 0:
                        writer.writerow(headers)

                    try:
                        writer.writerow(next(reader))
                        i += 1

                    except StopIteration:
                        records_exist = False

                        break

            if i == 0:
                os.remove(target_filepath)

            file_idx += 1


split_csv(
    source_filepath="data_given/aps_failure_test_set.csv",
    dest_folder="data_given/prediction_batch_files",
    records_per_file=3000,
)
