import pandas as pd


def main():
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", None)

    df_s = pd.read_csv("single-gf-data.csv")
    num_runs = len(df_s.index) / (
        df_s["num_processes"].nunique() * df_s["function"].nunique()
    )
    df_s = df_s.groupby(["function", "num_rows", "num_processes"], as_index=False)[
        ["time"]
    ].sum()
    df_s["time"] = df_s["time"].apply(lambda x: x / num_runs)
    df_s = df_s.pivot_table(index="num_rows", columns="function", values="time")
    df_s_plot = df_s.loc[:, :].plot.line(figsize=(10, 7))
    print(df_s)
    print(df_s_plot)

    df_m = pd.read_csv("multi-gf-data.csv")
    num_runs = len(df_m.index) / (
        df_m["dataset"].nunique() * df_m["function"].nunique()
    )
    df_m = df_m.groupby(["function", "total_rows", "dataset"], as_index=False)[
        ["time"]
    ].sum()
    df_m["time"] = df_m["time"].apply(lambda x: x / num_runs)
    df_m = df_m.pivot_table(index="dataset", columns="function", values="time")
    df_m_plot = df_m.loc[:, :].plot.line(figsize=(10, 7))
    print(df_m)
    print(df_m_plot)


if __name__ == "__main__":
    main()
