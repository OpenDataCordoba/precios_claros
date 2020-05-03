import argparse
import pandas as pd
from itertools import chain
from pathlib import Path


parser = argparse.ArgumentParser()
parser.add_argument(
    "src_path",
    metavar="paths",
    nargs="*",
    type=str,
    help="Path to files to be merged; enclose in quotes, accepts * as wildcard for directories or filenames",
)
parser.add_argument(
    "-o",
    type=str,
    default="productos.csv",
    help="Output csv",
)


args = parser.parse_args()

all_files = chain.from_iterable(
    Path('.').glob(p) if not p.startswith("/") else [Path(p)] for p in args.src_path
)

datasets = (pd.read_csv(f, index_col=None, header=0) for f in all_files)

frame = pd.concat(datasets, axis=0, ignore_index=True, sort=True).drop_duplicates(["id"]).sort_values(by="id")

target_cols = "id,marca,nombre,presentacion".split(",")
rest_cols = list(sorted(set(frame.columns) - set(target_cols)))
frame = frame.reindex(target_cols + rest_cols, axis=1)

frame.to_csv(args.o, index=False)
