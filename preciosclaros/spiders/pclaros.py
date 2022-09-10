# -*- coding: utf-8 -*-
import json
import math
from collections import defaultdict
from datetime import datetime
import scrapy
from preciosclaros.items import SucursalItem, ProductoItem, ProductoCategorizadoItem, PrecioItem


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

    def __init__(
        self,
        porcion="1/7",
        exportar=False,
        sucursales=1,
        max_sucursales_por_cadena=0,
        max_sucursales_criterio=None,
        productos=1,
        precios=1,
        ids="",
        *args,
        **kwargs,
    ):
        if "/" in porcion:
            self.porcion, self.total_spiders = [int(i) for i in porcion.split("/")]
            assert 1 <= self.porcion <= self.total_spiders
        else:
            self.porcion, self.total_spiders = 1, 1

        self.max_sucursales_por_cadena = int(max_sucursales_por_cadena)
        self.max_sucursales_criterio = max_sucursales_criterio
        self.sucursales_por_cadena = defaultdict(int)  # lleva la cuenta

        self.exportar = exportar

        # flags para skipear items
        self.sucursales = bool(int(sucursales))
        self.productos = bool(int(productos))
        self.precios = bool(int(precios))
        self.ids = [i.strip() for i in ids.split(",") if i.strip()]
        super().__init__(*args, **kwargs)

    def start_requests(self):
        import ipdb;ipdb.set_trace()
        if self.ids:
            for id_sucursal in self.ids:
                yield scrapy.Request(
                        url=productos_url + f"?limit={LIMIT_PRODUCTOS}&id_sucursal={id_sucursal}",
                        callback=self.parse_productos_first_page,
                        headers=HEADERS,
                        meta={"id_sucursal": id_sucursal},
                )
        else:
            yield scrapy.Request(
                url=sucursales_url + f"?limit={LIMIT_SUCURSALES}", callback=self.parse_sucursal_first_page, headers=HEADERS
            )

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

            # chequeo limite
            if self.max_sucursales_por_cadena:
                criterio = item.get(self.max_sucursales_criterio, "")
                id_cadena = f"{item['comercioId']}-{item['banderaId']}-{criterio}"
                self.sucursales_por_cadena[id_cadena] += 1
                if self.sucursales_por_cadena[id_cadena] > self.max_sucursales_por_cadena:
                    en = f" en la {self.max_sucursales_criterio} {criterio}" if self.max_sucursales_criterio else ""
                    msg = (
                        f"Se alcanzó el límite de {self.max_sucursales_por_cadena} "
                        f"sucursal/es para {item['comercioRazonSocial']}{en}. "
                        f"Sucursal {id_sucursal} ignorada."
                    )
                    self.logger.info(msg)
                    continue

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
        for items in self.parse_productos_y_precios(response, total):
            yield items
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


class CategoriasSpider(scrapy.Spider):
    name = "categorias"

    def __init__(
        self, *args, **kwargs,
    ):
        self.exportar = True

        # flags para skipear items
        self.sucursales = False  # bool(int(sucursales))
        self.productos = True  # bool(int(productos))
        self.precios = False  # bool(int(precios))
        self.data = {
            "01": "Alimentos Congelados",
            "02": "Almacén",
            "03": "Bebés",
            "04": "Bebidas con Alcohol",
            "05": "Bebidas sin Alcohol",
            "06": "Frescos",
            "07": "Limpieza",
            "08": "Mascotas",
            "09": "Perfumería y Cuidado Personal",
            "01-01": "Alimentos de Soja",
            "01-02": "Empanadas y Tartas",
            "01-03": "Hamburguesas",
            "01-04": "Helados",
            "01-05": "Pescados y Mariscos",
            "01-06": "Pizzas",
            "01-07": "Rebosados",
            "01-08": "Vegetales",
            "01-09": "Otros Alimentos Congelados",
            "02-01": "Aceites",
            "02-02": "Aceitunas y Encurtidos",
            "02-03": "Aderezos y Especias",
            "02-04": "Arroces, Legumbres y Semillas",
            "02-05": "Conservas",
            "02-06": "Desayuno y Merienda",
            "02-07": "Frutas Secas",
            "02-08": "Golosinas y Chocolates",
            "02-09": "Harinas y Pastas",
            "02-10": "Panificados",
            "02-11": "Para Preparar",
            "02-12": "Snacks",
            "02-13": "Sopas, Caldos y Puré",
            "02-14": "Suplemento Nutricional",
            "03-01": "Alimentación",
            "03-02": "Higiene y Accesorios",
            "04-01": "Aperitivos",
            "04-02": "Bebidas Blancas, Licores y Whisky",
            "04-03": "Cervezas",
            "04-04": "Espumantes",
            "04-05": "Vinos",
            "04-06": "Sidras y Otros",
            "05-01": "A Base de Hierbas",
            "05-02": "Aguas",
            "05-03": "Aguas Saborizadas",
            "05-04": "Cervezas y Sidras sin Alcohol",
            "05-05": "Gaseosas",
            "05-06": "Isotónicas y Energizantes",
            "05-07": "Jugos",
            "05-08": "Soda",
            "06-01": "Carnicería / Pollería",
            "06-02": "Pescadería",
            "06-03": "Fiambrería",
            "06-04": "Frutas",
            "06-05": "Huevos",
            "06-06": "Lácteos",
            "06-07": "Levaduras y Grasas",
            "06-08": "Pastas y Tapas",
            "06-09": "Verduras",
            "07-01": "Accesorios de Limpieza",
            "07-02": "Desodorantes y Desinfectantes de Ambientes",
            "07-03": "Insecticidas",
            "07-04": "Lavandinas",
            "07-05": "Limpieza de la Cocina",
            "07-06": "Limpieza de la Ropa",
            "07-07": "Limpieza del Baño",
            "07-08": "Limpieza del Calzado",
            "07-09": "Limpieza de Pisos y Muebles",
            "07-10": "Papeles",
            "08-01": "Mascotas",
            "09-01": "Cuidado Capilar",
            "09-02": "Cuidado Corporal",
            "09-03": "Cuidado Facial",
            "09-04": "Cuidado Oral",
            "09-05": "Productos Farmacéuticos",
            "09-06": "Productos para Adultos Mayores",
            "01-01-01": "Alimentos de Soja",
            "01-02-01": "Empanadas y Tartas",
            "01-03-01": "Hamburguesas",
            "01-04-01": "Helados",
            "01-05-01": "Pescados y Mariscos",
            "01-06-01": "Pizzas",
            "01-07-01": "De Pollo / Pescado",
            "01-08-01": "Vegetales",
            "01-09-01": "Otros Alimentos Congelados",
            "02-01-01": "Aerosol",
            "02-01-02": "Girasol",
            "02-01-03": "Maíz",
            "02-01-04": "Mezcla",
            "02-01-05": "Oliva",
            "02-01-06": "Otros Aceites",
            "02-02-01": "Aceitunas Negras",
            "02-02-02": "Aceitunas Rellenas",
            "02-02-03": "Aceitunas Verdes",
            "02-02-04": "Encurtidos",
            "02-03-01": "Aceto",
            "02-03-02": "Especias",
            "02-03-03": "Mayonesa",
            "02-03-04": "Mostaza",
            "02-03-05": "Saborizadores",
            "02-03-06": "Sal y Pimienta",
            "02-03-07": "Salsa Golf / Ketchup",
            "02-03-08": "Salsas",
            "02-03-09": "Vinagre",
            "02-03-10": "Jugo de Limón y Otros Aderezos",
            "02-04-01": "Arroz Blanco",
            "02-04-02": "Arroz Integral",
            "02-04-03": "Arroz Preparado",
            "02-04-04": "Legumbres",
            "02-04-05": "Maíz Pisingallo",
            "02-04-06": "Semillas",
            "02-04-07": "Otros Arroces, Legumbres y Semillas",
            "02-05-01": "Anchoas",
            "02-05-02": "Anchoas",
            "02-05-03": "Atún",
            "02-05-04": "Caballa",
            "02-05-05": "Frutas",
            "02-05-06": "Legumbres",
            "02-05-07": "Paté / Picadillo",
            "02-05-08": "Sardinas",
            "02-05-09": "Tomates / Salsas",
            "02-05-10": "Verduras",
            "02-05-11": "Otras Conservas",
            "02-06-01": "Azúcar",
            "02-06-02": "Cacao en Polvo / Chocolate para Taza",
            "02-06-03": "Café en Cápsulas",
            "02-06-04": "Café Instantáneo",
            "02-06-05": "Café Molido",
            "02-06-06": "Cereales / Avena",
            "02-06-07": "Dulce de Leche",
            "02-06-08": "Edulcorante",
            "02-06-09": "Filtros de Café",
            "02-06-10": "Galletitas de Arroz",
            "02-06-11": "Galletitas Dulces",
            "02-06-12": "Galletitas Saladas",
            "02-06-13": "Leche en Polvo",
            "02-06-14": "Mate Cocido",
            "02-06-15": "Mermeladas y Jaleas",
            "02-06-16": "Miel",
            "02-06-17": "Té",
            "02-06-18": "Yerba",
            "02-07-01": "Frutas Secas y Disecadas",
            "02-08-01": "Alfajores",
            "02-08-02": "Barras de Cereal",
            "02-08-03": "Bocaditos y Bombones",
            "02-08-04": "Caramelos, Chupetines y Chicles",
            "02-08-05": "Chocolates y Tabletas",
            "02-08-06": "Turrones y Confituras",
            "02-08-07": "Otras Golosinas",
            "02-09-01": "Almidón de Maíz",
            "02-09-02": "Fideos Guiseros y de Sopa",
            "02-09-03": "Fideos Largos",
            "02-09-04": "Harina de Maíz",
            "02-09-05": "Harina de Trigo",
            "02-09-06": "Pastas Rellenas",
            "02-09-07": "Sémola",
            "02-09-08": "Otras Pastas / Harinas",
            "02-10-01": "Budines / Bizcochuelos / Piononos",
            "02-10-02": "Hamburguesas y Panchos",
            "02-10-03": "Pan de Molde",
            "02-10-04": "Pan Francés",
            "02-10-05": "Pan Rallado y Rebozadores",
            "02-10-06": "Tostadas",
            "02-10-07": "Otros Productos Panificados",
            "02-11-01": "Bizcochuelos, Brownies y Tortas",
            "02-11-02": "Gelatinas",
            "02-11-03": "Mousse y Helados",
            "02-11-04": "Postres y Flanes",
            "02-11-05": "Premezclas",
            "02-11-06": "Repostería",
            "02-12-01": "Chizitos",
            "02-12-02": "Maní",
            "02-12-03": "Palitos",
            "02-12-04": "Papas fritas",
            "02-12-05": "Otros Snacks",
            "02-13-01": "Puré Instantáneo",
            "02-13-02": "Sopas, Caldos y Saborizadores",
            "02-14-01": "Suplemento Nutricional",
            "03-01-01": "Leche Infantil",
            "03-01-02": "Otros Productos para la Alimentación del Bebé",
            "03-02-01": "Aceites / Cremas / Jabones",
            "03-02-02": "Chupetes y Mamaderas",
            "03-02-03": "Pañales",
            "03-02-04": "Talco / Oleo Calcáreo",
            "03-02-05": "Toallitas Húmedas",
            "04-01-01": "Americano",
            "04-01-02": "Fernet",
            "04-01-03": "Otros Aperitivos",
            "04-02-01": "Bebidas Blancas",
            "04-02-02": "Licores",
            "04-02-03": "Whisky",
            "04-03-01": "En Botella",
            "04-03-02": "En Lata",
            "04-04-01": "Champagne",
            "04-04-02": "Frizantes",
            "04-05-01": "Vinos Blancos",
            "04-05-02": "Vinos Rosados",
            "04-05-03": "Vinos Tintos",
            "04-06-01": "Sidras",
            "04-06-02": "Otras Bebidas con Alcohol",
            "05-01-01": "Amargos",
            "05-02-01": "Gasificadas",
            "05-02-02": "Sin Gas",
            "05-03-01": "Gasificadas",
            "05-03-02": "Sin Gas",
            "05-04-01": "Cervezas y Sidras sin Alcohol",
            "05-05-01": "Cola",
            "05-05-02": "Lima Limón",
            "05-05-03": "Naranja",
            "05-05-04": "Pomelo",
            "05-05-05": "Tónica",
            "05-05-06": "Otras",
            "05-06-01": "",
            "05-07-01": "Concentrados",
            "05-07-02": "En Polvo",
            "05-07-03": "Listos",
            "05-08-01": "Soda",
            "06-01-01": "Carne de Cerdo",
            "06-01-02": "Carne Vacuna",
            "06-01-03": "Hígado y Achuras",
            "06-01-04": "Pollo",
            "06-01-05": "Otras Carnes",
            "06-02-01": "Pescados y Mariscos",
            "06-03-01": "Dulce de Batata / Membrillo",
            "06-03-02": "Fiambres",
            "06-03-03": "Salchichas / Embutidos",
            "06-03-04": "Otros Productos de Fiambrería",
            "06-04-01": "Ananá",
            "06-04-02": "Banana",
            "06-04-03": "Ciruela",
            "06-04-04": "Durazno",
            "06-04-05": "Kiwi",
            "06-04-06": "Limón",
            "06-04-07": "Mandarina",
            "06-04-08": "Manzana",
            "06-04-09": "Naranja",
            "06-04-10": "Pera",
            "06-04-11": "Pomelo",
            "06-04-12": "Uvas",
            "06-04-13": "Otras Frutas",
            "06-05-01": "Huevos",
            "06-06-01": "Crema de Leche",
            "06-06-02": "Leche Chocolatada",
            "06-06-03": "Leche Condensada",
            "06-06-04": "Leche Fluida Descremada",
            "06-06-05": "Leche Fluida Entera",
            "06-06-06": "Mantecas y Margarinas",
            "06-06-07": "Postres y Flanes",
            "06-06-08": "Quesos Blandos",
            "06-06-09": "Quesos Duros",
            "06-06-10": "Quesos Rallados",
            "06-06-11": "Quesos Semiduros",
            "06-06-12": "Quesos Untables",
            "06-06-13": "Ricota",
            "06-06-14": "Yogur Bebible",
            "06-06-15": "Yogur con Cereales / Frutas",
            "06-06-16": "Yogur Firme / Batido",
            "06-06-17": "Otras Leches Fluidas",
            "06-06-18": "Otros Quesos",
            "06-07-01": "Levaduras y Grasas",
            "06-08-01": "Fideos",
            "06-08-02": "Ñoquis",
            "06-08-03": "Pastas Rellenas",
            "06-08-04": "Tapas para Empanadas",
            "06-08-05": "Tapas para Tartas",
            "06-09-01": "Acelga",
            "06-09-02": "Ají",
            "06-09-03": "Ajo",
            "06-09-04": "Apio",
            "06-09-05": "Hinojo",
            "06-09-06": "Batata",
            "06-09-07": "Berenjenas",
            "06-09-08": "Cebolla Común",
            "06-09-09": "Cebolla de Verdeo, puerro",
            "06-09-10": "Coliflor, Brócoli",
            "06-09-11": "Lechuga",
            "06-09-12": "Palta",
            "06-09-13": "Papa Blanca",
            "06-09-14": "Papa Negra",
            "06-09-15": "Radicheta, Radicha, Rúcula",
            "06-09-16": "Repollo",
            "06-09-17": "Tomate Redondo",
            "06-09-18": "Zanahoria",
            "06-09-19": "Zapallitos Frescos",
            "06-09-20": "Zapallo",
            "06-09-21": "Otras Verduras",
            "07-01-01": "Baldes",
            "07-01-02": "Broches y Ganchos",
            "07-01-03": "Escobas / Escobillones / Palos / Cabos",
            "07-01-04": "Secadores y Cepillos",
            "07-01-05": "Esponjas y Guantes",
            "07-01-06": "Paños Multiuso / Trapos",
            "07-01-07": "Otros Accesorios de Limpieza",
            "07-02-01": "Absorbe Humedad",
            "07-02-02": "Aromatizantes",
            "07-02-03": "Desinfectantes",
            "07-02-04": "Desodorantes",
            "07-03-01": "Cucarachas y Hormigas",
            "07-03-02": "Moscas y Mosquitos",
            "07-03-03": "Polillas, Pulgas y Garrapatas",
            "07-03-04": "Repelentes",
            "07-03-05": "Roedores",
            "07-04-01": "Lavandina en Gel",
            "07-04-02": "Lavandina Líquida",
            "07-05-01": "Bolsas",
            "07-05-02": "Detergentes y Lavavajillas",
            "07-05-03": "Escarbadientes",
            "07-05-04": "Fósforos y Encendedores",
            "07-05-05": "Limpiadores",
            "07-05-06": "Limpiavidrios",
            "07-05-07": "Velas",
            "07-06-01": "Aprestos y Blanqueadores",
            "07-06-02": "Cepillos para la Ropa",
            "07-06-03": "Jabón en Pan",
            "07-06-04": "Jabón en Polvo",
            "07-06-05": "Jabón Líquido",
            "07-06-06": "Perfume para la Ropa",
            "07-06-07": "Quitamanchas",
            "07-06-08": "Suavizantes",
            "07-07-01": "Limpiadores / Desinfectantes Cremosos",
            "07-07-02": "Limpiadores / Desinfectantes en Aerosol",
            "07-07-03": "Limpiadores / Desinfectantes en Gel",
            "07-07-04": "Limpiadores / Desinfectantes Líquidos",
            "07-07-05": "Pastillas y Bloques",
            "07-08-01": "Brillos y Revividores",
            "07-08-02": "Limpiadores",
            "07-08-03": "Pomadas para el Calzado",
            "07-09-01": "Ceras y Autobrillos",
            "07-09-02": "Limpiadores de Pisos",
            "07-09-03": "Lustramuebles",
            "07-09-04": "Otros Productos para Limpiar Pisos y Muebles",
            "07-10-01": "Films",
            "07-10-02": "Pañuelos Descartables",
            "07-10-03": "Papel de Aluminio",
            "07-10-04": "Papel Higiénico",
            "07-10-05": "Rollos de Cocina",
            "07-10-06": "Servilletas",
            "08-01-01": "Alimentos para Gatos",
            "08-01-02": "Alimentos para Perros",
            "08-01-03": "Productos de Higiene para Mascotas",
            "08-01-04": "Otros Productos para Mascotas",
            "09-01-01": "Acondicionadores",
            "09-01-02": "Fijadores",
            "09-01-03": "Productos para Combatir la Pediculosis",
            "09-01-04": "Reparación y Tratamiento",
            "09-01-05": "Shampoo",
            "09-01-06": "Tintura",
            "09-02-01": "Algodones e Hisopos",
            "09-02-02": "Cepillos y Esponjas",
            "09-02-03": "Cremas de Manos y Corporales",
            "09-02-04": "Desodorantes Infantiles",
            "09-02-05": "Desodorante para el Hombre",
            "09-02-06": "Desodorante para la Mujer",
            "09-02-07": "Jabones de Tocador / Glicerina",
            "09-02-08": "Jabones Líquidos",
            "09-02-09": "Perfumes",
            "09-02-10": "Productos de Depilación",
            "09-02-11": "Protectores Diarios",
            "09-02-12": "Protectores Solares y Post Solares",
            "09-02-13": "Quitaesmaltes",
            "09-02-14": "Talcos",
            "09-02-15": "Tampones",
            "09-02-16": "Toallas Higiénicas",
            "09-02-17": "Otros Productos para el Cuidado Corporal",
            "09-03-01": "Cremas Antiacné",
            "09-03-02": "Cremas Antiarrugas",
            "09-03-03": "Cremas Hidratantes / Humectantes",
            "09-03-04": "Productos de Afeitar/ para después de Afeitarse",
            "09-03-05": "Desmaquillantes",
            "09-03-06": "Otros Productos para el Cuidado Facial",
            "09-04-01": "Accesorios Dentales",
            "09-04-02": "Cepillos Dentales",
            "09-04-03": "Enjuagues Bucales",
            "09-04-04": "Pastas Dentales",
            "09-05-01": "Alcohol",
            "09-05-02": "Antisépticos",
            "09-05-03": "Apósitos Protectores",
            "09-05-04": "Preservativos",
            "09-05-05": "Protectores Mamarios",
            "09-05-06": "Otros",
            "09-06-01": "Pañales para Adultos",
        }
        self.porcion = 1
        self.total_spiders = 1
        self.url = (
            productos_url
            + "?array_sucursales=15-1-1080,15-1-288,15-1-241,10-3-670,15-1-5173,10-3-621,10-3-587,9-3-121,9-2-30,9-1-119,9-3-5260,10-3-400,15-1-214,10-3-732,10-3-298,15-1-5197,10-3-526,12-1-116,9-2-247,15-1-90,10-3-615,15-1-446,10-3-380,10-3-563,15-1-382,10-3-616,10-3-370,10-3-533,10-3-561,15-1-492&limit=100&sort=-cant_sucursales_disponible"
        )
        super().__init__(*args, **kwargs)

    def start_requests(self):
        for cat, nombre_cat in self.data.items():
            if len(cat) < 6:
                # solo categorias terciarias
                continue
            yield scrapy.Request(
                url=self.url + f"&id_categoria={cat.replace('-', '')}",
                callback=self.parse_productos_first_page,
                meta={"cat": cat},
                headers=HEADERS,
            )

    def parse_productos_first_page(self, response):
        json_data = json.loads(response.text)
        total = json_data["total"]

        # procesar  items de la primera pagina ya solicitada
        for i in self.parse_productos_y_precios(response, total):
            yield i
        cat = response.meta["cat"]

        for offset in range(100, total, 100):
            yield scrapy.Request(
                url=self.url + f"&id_categoria={cat.replace('-', '')}&offset={offset}",
                callback=self.parse_productos_y_precios,
                headers=HEADERS,
                meta={"offset": offset, "total": total, "cat": cat},
            )

    def parse_productos_y_precios(self, response, total=None):
        json_data = json.loads(response.text)

        self.logger.info(
            "Obteniendo  %s/%s productos para la categoria %s",
            response.meta.get("offset", 100),
            response.meta.get("total", total),
            response.meta.get("cat"),
        )
        cat = response.meta.get("cat")
        try:
            productos = json_data["productos"]
        except KeyError:
            # loggeo el body del response para debugging
            self.logger.error(response.text)
            raise

        for prod in productos:
            prod.pop("precioMax")
            prod.pop("precioMin")
            prod.pop("cantSucursalesDisponible")
            producto = ProductoCategorizadoItem(prod)
            producto["categoria3"] = self.data[cat]
            producto["categoria2"] = self.data[cat.rpartition("-")[0]]
            producto["categoria1"] = self.data[cat.partition("-")[0]]
            yield producto
