import hatchet as ht
from hatchet.util import timer
import pandas as pd
import os
from natsort import natsorted


def main():
    path_to_directory = "../hatchet-data/quicksilver-only-time"
    directory = natsorted(os.listdir(path_to_directory))
    directory = [path for path in directory if "hpctoolkit" in path]

    num_processes = [64, 128, 256, 512]
    num_runs = 5
    metric = "REALTIME (sec) (I)"

    # average time for each process count
    file_read_times = []
    flat_profile_times = []
    load_imbalance_times = []
    hot_path_times = []

    for path in directory:
        # timers
        file_read_timer = timer.Timer()
        flat_profile_timer = timer.Timer()
        load_imbalance_timer = timer.Timer()
        hot_path_timer = timer.Timer()

        # total times
        file_read_tt = 0
        flat_profile_tt = 0
        load_imbalance_tt = 0
        hot_path_tt = 0

        for run in range(num_runs):
            # file read
            file_read_timer.start_phase(f"file read {run}")
            gf = ht.GraphFrame.from_hpctoolkit(path_to_directory + "/" + path)
            # gf = ht.GraphFrame.from_caliper("hatchet/tests/data/caliper-lulesh-json/lulesh-annotation-profile.json")
            file_read_timer.end_phase()
            file_read_tt += list(file_read_timer._times.values())[run].total_seconds()
            gf.default_metric = metric

            # flat profile
            flat_profile_timer.start_phase(f"flat_profile {run}")
            gf.flat_profile(metric)
            flat_profile_timer.end_phase()
            flat_profile_tt += list(flat_profile_timer._times.values())[
                run
            ].total_seconds()

            # load_imbalance
            load_imbalance_timer.start_phase(f"load_imbalance {run}")
            gf.load_imbalance(metric)
            load_imbalance_timer.end_phase()
            load_imbalance_tt += list(load_imbalance_timer._times.values())[
                run
            ].total_seconds()

            # hot_path
            hot_path_timer.start_phase(f"hot_path {run}")
            gf.hot_path(metric=metric)
            hot_path_timer.end_phase()
            hot_path_tt += list(hot_path_timer._times.values())[run].total_seconds()

        file_read_times.append(file_read_tt / num_runs)
        flat_profile_times.append(flat_profile_tt / num_runs)
        load_imbalance_times.append(load_imbalance_tt / num_runs)
        hot_path_times.append(hot_path_tt / num_runs)

    functions = [
        "file read",
        "flat_profile",
        "load_imbalance",
        "hot_path",
    ]
    times = [
        file_read_times,
        flat_profile_times,
        load_imbalance_times,
        hot_path_times,
    ]
    dataframes = []

    for i in range(len(functions)):
        data = {
            "num_processes": num_processes,
            "function": [functions[i]] * len(num_processes),
            "time": times[i],
        }
        dataframes.append(pd.DataFrame(data))

    df_all = pd.concat(dataframes)
    df_pivot = df_all.pivot(index="num_processes", columns="function", values="time")

    print(df_pivot)
    print(df_pivot.loc[:, :].plot.line(figsize=(10, 7)))


if __name__ == "__main__":
    main()
