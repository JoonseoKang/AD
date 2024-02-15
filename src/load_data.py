import os
import sys
from array import array
from collections import defaultdict
from datetime import datetime, timedelta

import matplotlib.pyplot as plt
import numpy as np
import zstd
from botocore.exceptions import ClientError
from tqdm import tqdm

sys.path.append(os.path.join("/".join(os.getcwd().split("/")), "src"))
import pickle

from DataLoader import DataLoader


def load_data(dl, config):
    query_results = dl.query(
        site=config["site"],
        process=config["process"],
        line=config["line"],
        equipment=config["equipment"],
        number=config["number"],
        start_time=config["start_time"],
        end_time=config["end_time"],
    )

    data_dict = defaultdict(dict)

    for result in tqdm(query_results):
        try:
            real_state = result[2][:27]
            phase = result[2][-5:-4]

            raw_data = dl.load(result)
            bytes_data = zstd.decompress(raw_data)
            float_data = np.array(array("f", bytes_data))

            data_dict[real_state][phase] = float_data[:]

        except ClientError as e:
            if e.response["Error"]["Code"] == "InternalError":
                continue
            else:
                print("An error occurred:", e)
    return data_dict

if __name__ == "__main__":
    dl = DataLoader()

    config = {
        "site": "eswa",
        "process": "lam",
        "line": "15",
        "equipment": "01",
        "number": 4,
        "start_time": "2023-04-15 00:00:00",
        "end_time": "2023-07-17 00:00:00",
        "data_shift_ind": 60,
    }

    # Convert start_time and end_time to datetime objects
    start_time = datetime.strptime(config["start_time"], "%Y-%m-%d %H:%M:%S")
    end_time = datetime.strptime(config["end_time"], "%Y-%m-%d %H:%M:%S")

    # Iterate over each day from start_time to end_time
    current_time = start_time
    while current_time <= end_time:
        # Set start_time and end_time to the current day and the next day
        config["start_time"] = current_time.strftime("%Y-%m-%d %H:%M:%S")
        config["end_time"] = (current_time + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

        # Call load_data and store the result in data_dict
        data_dict = load_data(dl, config)

        # Open a pickle file named with the current day and dump data_dict into it
        with open(f'/home/joonseo/AD/data/raw/15_1_4_{current_time.strftime("%m%d")}.pkl', 'wb') as f:
            pickle.dump(data_dict, f)

        # Move to the next day
        current_time += timedelta(days=1)
    
        
        
