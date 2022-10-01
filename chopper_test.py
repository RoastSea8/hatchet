import os
import pandas as pd
import hatchet as ht
from natsort import natsorted
from hatchet.util import timer


def main():
    # ---------------------------------------------------------------------------------
    """SINGLE-GRAPHFRAME VARIABLES"""
    path_to_directory = "../hatchet-data/quicksilver-only-time"
    sgf_data_filename = "single-gf-data.csv"
    reader_function = ht.GraphFrame.from_hpctoolkit
    metric = "REALTIME (sec) (I)"
    num_processes = [64, 128, 256, 512]
    # single-graphframe functions
    single_gf_functions = [
        "file read",
        "to_callgraph",
        "load_imbalance",
        "hot_path",
    ]

    """MULTI-GRAPHFRAME VARIABLES"""
    # replace keys with dataset names to appear on x-axis of plot
    mgf_data_filename = "multi-gf-data.csv"
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

    # single-graphframe function tests
    directory = natsorted(os.listdir(path_to_directory))
    single_gf_timer = timer.Timer()
    num_rows_list = []
    num_processes_list = []

    for path in directory:
        # file read
        if "file read" in single_gf_functions:
            with single_gf_timer.phase("file read {}".format(directory.index(path))):
                gf = reader_function(path_to_directory + "/" + path)
            gf.default_metric = metric

        # to_callgraph
        if "to_callgraph" in single_gf_functions:
            gf_copy = gf.deepcopy()
            gf_copy.drop_index_levels()
            with single_gf_timer.phase("to_callgraph {}".format(directory.index(path))):
                gf_copy.to_callgraph()

        # load_imbalance
        if "load_imbalance" in single_gf_functions:
            with single_gf_timer.phase(
                "load_imbalance {}".format(directory.index(path))
            ):
                gf.load_imbalance(metric_columns=metric)

        # hot_path
        if "hot_path" in single_gf_functions:
            with single_gf_timer.phase("hot_path {}".format(directory.index(path))):
                gf.hot_path(metric=metric)

        for f in single_gf_functions:
            num_processes_list.append(num_processes[directory.index(path)])
            num_rows_list.append(gf.dataframe.shape[0])

    times = single_gf_timer.__str__().split("\n")
    times = times[1 : len(times) - 1]
    function_list = []
    time_list = []
    for i in range(len(times)):
        function_list.append(times[i].split(":")[0].strip())
        time_list.append(float((times[i].split(":")[1].strip())[:-1]))

    data = {}
    data["function"] = [f[:-2] for f in function_list]
    data["time"] = time_list
    data["num_rows"] = num_rows_list
    data["num_processes"] = num_processes_list

    dataframe = pd.DataFrame(data)

    try:
        df_old = pd.read_csv(sgf_data_filename)
        if not df_old.equals(dataframe):
            df_new = pd.concat([df_old, dataframe])
            df_new = df_new[["function", "time", "num_rows", "num_processes"]]
            df_new.to_csv(sgf_data_filename)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        dataframe.to_csv(sgf_data_filename)

    # multi-graphframe function tests
    multi_gf_timer = timer.Timer()
    dataset_list = []
    total_rows_list = []

    for dataset in paths_to_directories.keys():
        # construct_from
        if "construct_from" in multi_gf_functions:
            directory = natsorted(os.listdir(paths_to_directories[dataset]["path"]))
            directory = [
                paths_to_directories[dataset]["path"] + "/" + path
                for path in directory
                if "DS" not in path
            ]
            with multi_gf_timer.phase(
                "construct_from {}".format(
                    list(paths_to_directories.keys()).index(dataset)
                )
            ):
                gfs = ht.GraphFrame.construct_from(directory)
            total_rows = 0
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
                total_rows += gf.dataframe.shape[0]

        # multirun_analysis
        if "multirun_analysis" in multi_gf_functions:
            with multi_gf_timer.phase(
                "multirun_analysis {}".format(
                    list(paths_to_directories.keys()).index(dataset)
                )
            ):
                ht.Chopper.multirun_analysis(
                    graphframes=gfs,
                    pivot_index="num_processes",
                    columns="name",
                    metric=paths_to_directories[dataset]["metric"],
                    threshold=0,
                )

        # calculate_speedup_efficiency
        if "calculate_speedup_efficiency" in multi_gf_functions:
            with multi_gf_timer.phase(
                "calculate_speedup_efficiency {}".format(
                    list(paths_to_directories.keys()).index(dataset)
                )
            ):
                ht.Chopper.calculate_speedup_efficiency(
                    graphframes=gfs, metric=paths_to_directories[dataset]["metric"]
                )

        for f in multi_gf_functions:
            dataset_list.append(dataset)
            total_rows_list.append(total_rows)

    times = multi_gf_timer.__str__().split("\n")
    times = times[1 : len(times) - 1]
    function_list = []
    time_list = []
    for i in range(len(times)):
        function_list.append(times[i].split(":")[0].strip())
        time_list.append(float((times[i].split(":")[1].strip())[:-1]))

    data = {}
    data["function"] = [f[:-2] for f in function_list]
    data["time"] = time_list
    data["dataset"] = dataset_list
    data["total_rows"] = total_rows_list

    dataframe = pd.DataFrame(data)

    try:
        df_old = pd.read_csv(mgf_data_filename)
        if not df_old.equals(dataframe):
            df_new = pd.concat([df_old, dataframe])
            df_new = df_new[["function", "time", "dataset", "total_rows"]]
            df_new.to_csv(mgf_data_filename)
    except (pd.errors.EmptyDataError, FileNotFoundError):
        dataframe.to_csv(mgf_data_filename)


if __name__ == "__main__":
    main()
