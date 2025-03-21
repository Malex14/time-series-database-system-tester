import sys

import pandas as pd
from os.path import isfile, join, basename
from os import listdir

def main():
    if len(sys.argv) != 2:
        print("Usage: python convert_dataset_scenario_b.py <path to downloaded dataset>")
        sys.exit(1)

    path = sys.argv[1]
    out = "./dataset/scenario_b"

    files = [f for f in listdir(path) if isfile(join(path, f)) and f.endswith(".parquet")]

    for file in files:
        filepath = join(path, file)
        print(filepath)
        df2 = pd.read_parquet(filepath)
        dfc = df2[["tpep_pickup_datetime", "passenger_count", "trip_distance", "total_amount"]].drop_duplicates(
            subset='tpep_pickup_datetime', keep='last')
        dfc.to_parquet(join(out, file))

if __name__ == '__main__':
    main()