import argparse
import pandas as pd
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument(
    "src_path",
    metavar="path",
    type=str,
    help="Path to files to be merged; enclose in quotes, accepts * as wildcard for directories or filenames",
)
parser.add_argument("-o", type=str, default="precios_{date_from}_{date_to}.csv", help="Output csv")
args = parser.parse_args()

all_files = Path(".").glob(args.src_path)

datasets = (pd.read_csv(f, parse_dates=["fecha_relevamiento"], index_col=None, header=0) for f in all_files)
frame = pd.concat(datasets, axis=0, ignore_index=True)
date_from = frame.fecha_relevamiento.min().strftime("%Y%m%d")
date_to = frame.fecha_relevamiento.max().strftime("%Y%m%d")

# dejar solo un valor por sucursal
frame["cadena"] = frame.sucursal_id.str.rpartition("-")[0]
frame = frame.drop_duplicates(["cadena", "precio", "producto_id"])

frame = frame.drop(["cadena", "fecha_relevamiento", "precio_max", "precio_min"], axis=1)
frame = frame.sort_values(by=["producto_id", "sucursal_id"])

frame.to_csv(args.o.format(date_from=date_from, date_to=date_to), index=False)
