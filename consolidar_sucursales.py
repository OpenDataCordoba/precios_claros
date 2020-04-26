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
parser.add_argument(
    "-o",
    type=str,
    default="sucursales.csv",
    help="Output csv",
)


args = parser.parse_args()

all_files = Path(".").glob(args.src_path)

datasets = (pd.read_csv(f, index_col=None, header=0) for f in all_files)

frame = pd.concat(datasets, axis=0, ignore_index=True).drop_duplicates().sort_values(by="id")

frame = frame["id,comercioId,banderaId,banderaDescripcion,comercioRazonSocial,provincia,localidad,direccion,lat,lng,sucursalNombre,sucursalTipo".split(",")]

frame.to_csv(args.o, index=False)
