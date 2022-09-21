import os
import json
import pandas as pd
import hatchet as ht
from natsort import natsorted
from hatchet.util import timer


def reset_time(data):
    for function in data.keys():
        data[function]["timer"] = timer.Timer()
        data[function]["total_time"] = 0


def init_json(filename):
    try:
        with open(str(filename)) as f:
            json_data = json.load(f)
    except FileNotFoundError:
        with open(str(filename), "w") as f:
            json_data = {}
    return json_data


def main():

    # ---------------------------------------------------------------------------------
    json_data = init_json("data.json")
    path_to_directory = "../hatchet-data/quicksilver-only-time"
    # data identifier must be present in filename
    data_identifier = "hpctoolkit"
    reader_function = ht.GraphFrame.from_hpctoolkit
    metric = "REALTIME (sec) (I)"
    num_runs = 5
    num_processes = [64, 128, 256, 512]
    functions = [
        "file read",
        "flat_profile",
        # "flatten",
        # "to_callgraph",
        "load_imbalance",
        "hot_path",
    ]
    # ---------------------------------------------------------------------------------

    directory = natsorted(os.listdir(path_to_directory))
    directory = [path for path in directory if data_identifier in path]
    data = {}
    for function in functions:
        data[function] = dict(
            {
                "timer": timer.Timer(),
                "total_time": 0,
                # list of average times for each process count
                "times_list": [],
                # number rows in dataframes for each dataset
                "Rows in Dataframe": [],
            }
        )
    dataframes = []

    for path in directory:
        reset_time(data)
        for run in range(num_runs):
            # file read
            if "file read" in functions:
                data["file read"]["timer"].start_phase(f"file read {run}")
                gf = reader_function(path_to_directory + "/" + path)
                data["file read"]["timer"].end_phase()
                data["file read"]["total_time"] += list(
                    data["file read"]["timer"]._times.values()
                )[run].total_seconds()
                gf.default_metric = metric

            # flat profile
            if "flat_profile" in functions:
                data["flat_profile"]["timer"].start_phase(f"flat_profile {run}")
                gf.flat_profile()
                data["flat_profile"]["timer"].end_phase()
                data["flat_profile"]["total_time"] += list(
                    data["flat_profile"]["timer"]._times.values()
                )[run].total_seconds()

            # flatten
            if "flatten" in functions:
                gf_copy = gf.deepcopy()
                data["flatten"]["timer"].start_phase(f"flatten {run}")
                gf_copy.flatten()
                data["flatten"]["timer"].end_phase()
                data["flatten"]["total_time"] += list(
                    data["flatten"]["timer"]._times.values()
                )[run].total_seconds()

            # to_callgraph
            if "to_callgraph" in functions:
                gf_copy = gf.deepcopy()
                gf_copy.drop_index_levels()
                data["to_callgraph"]["timer"].start_phase(f"to_callgraph {run}")
                gf_copy.to_callgraph()
                data["to_callgraph"]["timer"].end_phase()
                data["to_callgraph"]["total_time"] += list(
                    data["to_callgraph"]["timer"]._times.values()
                )[run].total_seconds()

            # load_imbalance
            if "load_imbalance" in functions:
                data["load_imbalance"]["timer"].start_phase(f"load_imbalance {run}")
                gf.load_imbalance(metric_columns=metric)
                data["load_imbalance"]["timer"].end_phase()
                data["load_imbalance"]["total_time"] += list(
                    data["load_imbalance"]["timer"]._times.values()
                )[run].total_seconds()

            # hot_path
            if "hot_path" in functions:
                data["hot_path"]["timer"].start_phase(f"hot_path {run}")
                gf.hot_path(metric=metric)
                data["hot_path"]["timer"].end_phase()
                data["hot_path"]["total_time"] += list(
                    data["hot_path"]["timer"]._times.values()
                )[run].total_seconds()

        for function in functions:
            data[function]["Rows in Dataframe"].append(gf.dataframe.shape[0])
            data[function]["times_list"].append(data[function]["total_time"] / num_runs)

    for function in functions:
        d = {
            "num_processes": num_processes,
            "Rows in Dataframe": data[function]["Rows in Dataframe"],
            "function": [function] * len(num_processes),
            "time": data[function]["times_list"],
        }
        json_data[function] = d
        with open("data.json", "w") as f:
            json.dump(json_data, f)

    for func_data in json_data.values():
        dataframes.append(pd.DataFrame(func_data))

    df_all = pd.concat(dataframes)
    df_pivot = df_all.pivot_table(
        index="Rows in Dataframe", columns="function", values="time"
    )
    df_plot = df_pivot.loc[:, :].plot.line(figsize=(10, 7))

    print(df_pivot)
    print(df_plot)


if __name__ == "__main__":
    main()
