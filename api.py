from pathlib import Path
import functools
from subprocess import run
import pandas as pd

iso_codes = {
    "AR-A": "Salta",
    "AR-B": "Provincia de Buenos Aires",
    "AR-C": "Ciudad Autónoma de Buenos Aires",
    "AR-D": "San Luis",
    "AR-E": "Entre Ríos",
    "AR-F": "La Rioja",
    "AR-G": "Santiago del Estero",
    "AR-H": "Chaco",
    "AR-J": "San Juan",
    "AR-K": "Catamarca",
    "AR-L": "La Pampa",
    "AR-M": "Mendoza",
    "AR-N": "Misiones",
    "AR-P": "Formosa",
    "AR-Q": "Neuquén",
    "AR-R": "Río Negro",
    "AR-S": "Santa Fe",
    "AR-T": "Tucumán",
    "AR-U": "Chubut",
    "AR-V": "Tierra del Fuego",
    "AR-W": "Corrientes",
    "AR-X": "Córdoba",
    "AR-Y": "Jujuy",
    "AR-Z": "Santa Cruz",
}


def download_data():
    run(["kaggle", "datasets", "download", "--unzip", "tinnqn/precios-claros-precios-de-argentina"])


def sucursales_prov():

    sucursales = pd.read_csv("sucursales.csv")

    sucursales_prov = sucursales[["id", "provincia", "banderaDescripcion"]]
    sucursales_prov.rename({"banderaDescripcion": "cadena"}, axis=1, inplace=True)
    sucursales_prov.replace({"provincia": iso_codes}, inplace=True)
    sucursales_prov["id_prov"] = (
        sucursales["comercioId"].astype(str) + "-" + sucursales["banderaId"].astype(str) + "-" + sucursales["provincia"]
    )
    sucursales_prov.set_index("id", inplace=True)
    return sucursales_prov


def read_precio(f, sucursales_df):
    fecha = f.name.split("_")[-1][:-4]
    precio = pd.read_csv(f)
    precio.rename(columns={"precio": f"precio_{fecha}"}, inplace=True)
    precio = precio.join(sucursales_df, on="sucursal_id")
    return precio


def read_precios():
    sucursales = sucursales_prov()
    productos = pd.read_csv("productos.csv")
    precios = [read_precio(f, sucursales) for f in Path(".").glob("precios_*.csv")]

    # aplico merges de todos los precios
    def merge(left, right):
        return (
            pd.merge(left, right, on=["producto_id", "id_prov", "provincia"], how="inner").dropna()
            # .drop(["sucursal_id_x", "sucursal_id_y"], axis=1)
        )

    precios = functools.reduce(merge, precios)
    for c in precios.columns:
        if c.startswith(("sucursal", "cadena_")):
            precios.drop(c, axis=1, inplace=True)

    # cruzamos datos con productos
    precios = pd.merge(
        precios,
        productos[["id", "marca", "nombre", "categoria1", "categoria2", "categoria3"]],
        left_on="producto_id",
        right_on="id",
    ).drop(["id", "id_prov"], axis=1)

    precios.sort_index(axis=1, inplace=True)

    inicio, *_, fin = [c for c in precios.columns if c.startswith("precio_")]

    precios["variacion"] = precios[fin] - precios[inicio]
    precios["variacion_relativa"] = precios["variacion"].abs() / precios[inicio] * 100
    return precios
