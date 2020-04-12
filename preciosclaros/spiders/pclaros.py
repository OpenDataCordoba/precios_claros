# -*- coding: utf-8 -*-
import json
import math
from datetime import datetime
import scrapy
from preciosclaros.items import SucursalItem, ProductoItem, PrecioItem


HEADERS = {
    "x-api-key": "zIgFou7Gta7g87VFGL9dZ4BEEs19gNYS1SOQZt96",
    "referer": "https://www.preciosclaros.gob.ar",
    "sec-fetch-mode": "cors",
    "user-agent": "User-Agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36",
}

base_url = "https://d3e6htiiul5ek9.cloudfront.net/"
sucursales_url = base_url + "prod/sucursales"  # ?limit=50&offset=50
productos_url = base_url + "prod/productos"  # ?id_sucursal


# en su membresia gratuita de Scrapinghub se impone un limite de 24hs de ejecucion por job,
# y en ese periodo se obtienen 4millones de items, que es aproximadamente la mitad de los datos que ofrece
# el portal precios claros
#
# Haciendo un *abuso bienintencionado* (?), lo que hago es dividir es obtener 1/7 partes del dataset
# (aproximadamente) por cada ejecucion, a traves de un argumento que se pasa el scrapper,
# programando la corrida de una porcion diferente cada dia.
#


LIMIT_SUCURSALES = 30
LIMIT_PRODUCTOS = 50

class PreciosClarosSpider(scrapy.Spider):
    name = "preciosclaros"

    def __init__(self, porcion="1/7", exportar=False, sucursales=1, productos=1, precios=1, *args, **kwargs):
        if "/" in porcion:
            self.porcion, self.total_spiders = [int(i) for i in porcion.split("/")]
            assert 1 <= self.porcion <= self.total_spiders
        else:
            self.porcion, self.total_spiders = 1, 1

        self.exportar = exportar
        self.sucursales = bool(int(sucursales))
        self.productos = bool(int(productos))
        self.precios = bool(int(precios))
        super().__init__(*args, **kwargs)

    def start_requests(self):
        yield scrapy.Request(url=sucursales_url + f"?limit={LIMIT_SUCURSALES}", callback=self.parse_sucursal_first_page, headers=HEADERS)

    def parse_sucursal_first_page(self, response):
        """
        A traves del response de la primer pagina ``sucursales_url`` obtiene el total de sucursales y
        calcula cada peticion secuencialmente.

        el total se divide en la cantidad de spiders y se redondea para que
        cada cada uno scrappee su "grupo" de sucursales,
        en páginas de tamaño ``LIMIT``
        """
        json_data = json.loads(response.text)
        total = json_data["total"]

        sucursales_por_spider = int(math.ceil((total / self.total_spiders)))
        self.logger.info(f"{total} sucursales en {self.total_spiders} spiders: {sucursales_por_spider} por spider")

        if not self.sucursales:
            return
        start = sucursales_por_spider * (self.porcion - 1)
        end = start + sucursales_por_spider

        # process pages
        for offset in range(start, end, LIMIT_SUCURSALES):
            yield scrapy.Request(
                url=sucursales_url + f"?limit={LIMIT_SUCURSALES}&offset={offset}",
                callback=self.parse_sucursal,
                headers=HEADERS,
                meta={"offset": offset, "end": end},
            )

    def parse_sucursal(self, response):
        """
        Este metodo es el parser real de la pagina para obtener sucursales.
        El item se interpreta *as is* como lo ofrece la API.

        Luego, por cada sucursal se genera un request a la primer
        pagina de los productos
        """
        self.logger.info("Obteniendo sucursales %s/%s", response.meta.get("offset"), response.meta.get("end"))
        sucursales = json.loads(response.text)["sucursales"]
        for suc in sucursales:
            item = SucursalItem(suc)
            id_sucursal = item["id"]
            yield item
            if self.productos:
                yield scrapy.Request(
                    url=productos_url + f"?limit={LIMIT_PRODUCTOS}&id_sucursal={id_sucursal}",
                    callback=self.parse_productos_first_page,
                    headers=HEADERS,
                    meta={"id_sucursal": id_sucursal},
                )

    def parse_productos_first_page(self, response):
        json_data = json.loads(response.text)
        total = json_data["total"]
        # procesar  items de la primera pagina ya solicitada
        self.parse_productos_y_precios(response, total)
        id_sucursal = response.meta["id_sucursal"]

        for offset in range(LIMIT_PRODUCTOS, total, LIMIT_PRODUCTOS):
            yield scrapy.Request(
                url=productos_url + f"?limit={LIMIT_PRODUCTOS}&offset={offset}&id_sucursal={id_sucursal}",
                callback=self.parse_productos_y_precios,
                headers=HEADERS,
                meta={"offset": offset, "total": total, "id_sucursal": response.meta["id_sucursal"]},
            )

    def parse_productos_y_precios(self, response, total=None):
        json_data = json.loads(response.text)
        self.logger.info(
            "Obteniendo  %s/%s precios para la sucursal %s",
            response.meta.get("offset", 50),
            response.meta.get("total", total),
            response.meta.get("id_sucursal"),
        )
        self.parse_sucursal(response)
        try:
            productos = json_data["productos"]
        except KeyError:
            # loggeo el body del response para debugging
            self.logger.error(response.text)
            raise

        items = []
        for prod in productos:
            precio = prod.pop("precio")
            precio_max = prod.pop("precioMax")
            precio_min = prod.pop("precioMin")

            producto = ProductoItem(prod)
            items.append(producto)
            if self.precios:
                items.append(
                    PrecioItem(
                        precio=precio,
                        precio_max=precio_max,
                        precio_min=precio_min,
                        sucursal_id=response.meta["id_sucursal"],
                        producto_id=producto["id"],
                        fecha_relevamiento=datetime.utcnow(),
                    )
                )
        return items
