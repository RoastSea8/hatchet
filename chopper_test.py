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
    reader_function = ht.GraphFrame.from_hpctoolkit
    metric = "REALTIME (sec) (I)"
    num_runs = 5
    num_processes = [64, 128, 256, 512]
    # single-graphframe functions
    functions = [
        "file read",
        "to_callgraph",
        "load_imbalance",
        "hot_path",
    ]

    # replace keys with dataset names to appear on x-axis of plot
    paths_to_directories = {
        "LULESH": {
            "path": "../hatchet-data/caliper-lulesh-json",
            "num_processes": [1, 8, 27, 64, 125, 216, 343, 512],
            "metric": "time",
        },
        "quicksilver": {
            "path": "../hatchet-data/quicksilver-only-time",
            "num_processes": [64, 128, 256, 512],
            "metric": "REALTIME (sec) (I)",
        },
    }
    # multi-graphframe functions
    multi_gf_functions = [
        "construct_from",
        "multirun_analysis",
        "calculate_speedup_efficiency",
    ]
    # ---------------------------------------------------------------------------------

    directory = natsorted(os.listdir(path_to_directory))
    data = {}
    for function in functions:
        data[function] = dict(
            {
                "timer": timer.Timer(),
                "total_time": 0,
                # list of average times for each process count
                "times_list": [],
                # number of rows in dataframes for each dataset
                "num_rows": [],
            }
        )
    for function in multi_gf_functions:
        data[function] = dict(
            {
                "timer": timer.Timer(),
                "total_time": 0,
                # list of average times for each process count
                "times_list": [],
                # names of datasets that function is called on
                "dataset_names": [],
            }
        )

    dataframes = []

    # single-graphframe function tests
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
            data[function]["num_rows"].append(gf.dataframe.shape[0])
            data[function]["times_list"].append(data[function]["total_time"] / num_runs)

    # multi-graphframe function tests
    for dataset in paths_to_directories.keys():
        reset_time(data)
        for run in range(num_runs):
            # construct_from
            if "construct_from" in multi_gf_functions:
                directory = natsorted(os.listdir(paths_to_directories[dataset]["path"]))
                directory = [
                    paths_to_directories[dataset]["path"] + "/" + path
                    for path in directory
                ]
                data["construct_from"]["timer"].start_phase(f"construct_from {run}")
                gfs = ht.GraphFrame.construct_from(directory)
                data["construct_from"]["timer"].end_phase()
                data["construct_from"]["total_time"] += list(
                    data["construct_from"]["timer"]._times.values()
                )[run].total_seconds()
                for gf in gfs:
                    gf.default_metric = paths_to_directories[dataset]["metric"]
                    # check that number of given process counts is equal to number of graphframes generated
                    assert len(gfs) == len(
                        paths_to_directories[dataset]["num_processes"]
                    ), f"number of process counts does not match number of graphframes for {dataset}"
                    gf.update_metadata(
                        num_processes=paths_to_directories[dataset]["num_processes"][
                            gfs.index(gf)
                        ]
                    )

            # multirun_analysis
            if "multirun_analysis" in multi_gf_functions:
                data["multirun_analysis"]["timer"].start_phase(
                    f"multirun_analysis {run}"
                )
                ht.GraphFrame.multirun_analysis(
                    graphframes=gfs,
                    pivot_index="num_processes",
                    columns="name",
                    metric=paths_to_directories[dataset]["metric"],
                    threshold=0,
                )
                data["multirun_analysis"]["timer"].end_phase()
                data["multirun_analysis"]["total_time"] += list(
                    data["multirun_analysis"]["timer"]._times.values()
                )[run].total_seconds()

            # calculate_speedup_efficiency
            if "calculate_speedup_efficiency" in multi_gf_functions:
                data["calculate_speedup_efficiency"]["timer"].start_phase(
                    f"calculate_speedup_efficiency {run}"
                )
                ht.GraphFrame.calculate_speedup_efficiency(
                    graphframes=gfs, metric=paths_to_directories[dataset]["metric"]
                )
                data["calculate_speedup_efficiency"]["timer"].end_phase()
                data["calculate_speedup_efficiency"]["total_time"] += list(
                    data["calculate_speedup_efficiency"]["timer"]._times.values()
                )[run].total_seconds()

        for function in multi_gf_functions:
            data[function]["dataset_names"].append(dataset)
            data[function]["times_list"].append(data[function]["total_time"] / num_runs)

    for function in functions:
        d = {
            "num_processes": num_processes,
            "Rows in Dataframe": data[function]["num_rows"],
            "function": [function] * len(num_processes),
            "time": data[function]["times_list"],
        }
        json_data[function] = d
        with open("data.json", "w") as f:
            json.dump(json_data, f)

    # single-graphframe functions plot
    for function, function_data in json_data.items():
        if function in functions:
            dataframes.append(pd.DataFrame(function_data))

    df_all = pd.concat(dataframes)
    df_pivot = df_all.pivot_table(
        index="Rows in Dataframe", columns="function", values="time"
    )
    df_plot = df_pivot.loc[:, :].plot.line(figsize=(10, 7))

    print(df_pivot)
    print(df_plot)

    dataframes.clear()

    # multi-graphframe functions plot
    for function in multi_gf_functions:
        d = {
            "dataset": data[function]["dataset_names"],
            "function": [function] * len(paths_to_directories),
            "time": data[function]["times_list"],
        }
        json_data[function] = d
        with open("data.json", "w") as f:
            json.dump(json_data, f)

    for function, data in json_data.items():
        if function in multi_gf_functions:
            dataframes.append(pd.DataFrame(data))

    df_all = pd.concat(dataframes)
    df_pivot = df_all.pivot_table(index="dataset", columns="function", values="time")
    df_plot = df_pivot.loc[:, :].plot.line(figsize=(10, 7))

    print(df_pivot)
    print(df_plot)


if __name__ == "__main__":
    main()
