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
        super().__init__(*args, **kwargs)

    def start_requests(self):
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
        for i in self.parse_productos_y_precios(response, total):
            yield i
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
            "02-10": "Aceites",
            "02-11": "Aceitunas y Encurtidos",
            "02-12": "Aderezos y Especias",
            "02-13": "Arroces, Legumbres y Semillas",
            "02-14": "Conservas",
            "02-15": "Desayuno y Merienda",
            "02-16": "Frutas Secas",
            "02-17": "Golosinas y Chocolates",
            "02-18": "Harinas y Pastas",
            "02-19": "Panificados",
            "02-20": "Para Preparar",
            "02-21": "Snacks",
            "02-22": "Sopas, Caldos y Puré",
            "02-23": "Suplemento Nutricional",
            "03-24": "Alimentación",
            "03-25": "Higiene y Accesorios",
            "04-26": "Aperitivos",
            "04-27": "Bebidas Blancas, Licores y Whisky",
            "04-28": "Cervezas",
            "04-29": "Espumantes",
            "04-30": "Vinos",
            "04-31": "Sidras y Otros",
            "05-32": "A Base de Hierbas",
            "05-33": "Aguas",
            "05-34": "Aguas Saborizadas",
            "05-35": "Cervezas y Sidras sin Alcohol",
            "05-36": "Gaseosas",
            "05-37": "Isotónicas y Energizantes",
            "05-38": "Jugos",
            "05-39": "Soda",
            "06-40": "Carnicería / Pollería",
            "06-41": "Pescadería",
            "06-42": "Fiambrería",
            "06-43": "Frutas",
            "06-44": "Huevos",
            "06-45": "Lácteos",
            "06-46": "Levaduras y Grasas",
            "06-47": "Pastas y Tapas",
            "06-48": "Verduras",
            "07-49": "Accesorios de Limpieza",
            "07-50": "Desodorantes y Desinfectantes de Ambientes",
            "07-51": "Insecticidas",
            "07-52": "Lavandinas",
            "07-53": "Limpieza de la Cocina",
            "07-54": "Limpieza de la Ropa",
            "07-55": "Limpieza del Baño",
            "07-56": "Limpieza del Calzado",
            "07-57": "Limpieza de Pisos y Muebles",
            "07-58": "Papeles",
            "08-59": "Mascotas",
            "09-60": "Cuidado Capilar",
            "09-61": "Cuidado Corporal",
            "09-62": "Cuidado Facial",
            "09-63": "Cuidado Oral",
            "09-64": "Productos Farmacéuticos",
            "09-65": "Productos para Adultos Mayores",
            "01-01-01": "Alimentos de Soja",
            "01-02-01": "Empanadas y Tartas",
            "01-03-01": "Hamburguesas",
            "01-04-01": "Helados",
            "01-05-01": "Pescados y Mariscos",
            "01-06-01": "Pizzas",
            "01-07-01": "De Pollo / Pescado",
            "01-08-01": "Vegetales",
            "01-09-01": "Otros Alimentos Congelados",
            "02-10-01": "Aerosol",
            "02-10-02": "Girasol",
            "02-10-03": "Maíz",
            "02-10-04": "Mezcla",
            "02-10-05": "Oliva",
            "02-10-06": "Otros Aceites",
            "02-11-01": "Aceitunas Negras",
            "02-11-02": "Aceitunas Rellenas",
            "02-11-03": "Aceitunas Verdes",
            "02-11-04": "Encurtidos",
            "02-12-01": "Aceto",
            "02-12-02": "Especias",
            "02-12-03": "Mayonesa",
            "02-12-04": "Mostaza",
            "02-12-05": "Saborizadores",
            "02-12-06": "Sal y Pimienta",
            "02-12-07": "Salsa Golf / Ketchup",
            "02-12-08": "Salsas",
            "02-12-09": "Vinagre",
            "02-12-10": "Jugo de Limón y Otros Aderezos",
            "02-13-01": "Arroz Blanco",
            "02-13-02": "Arroz Integral",
            "02-13-03": "Arroz Preparado",
            "02-13-04": "Legumbres",
            "02-13-05": "Maíz Pisingallo",
            "02-13-06": "Semillas",
            "02-13-07": "Otros Arroces, Legumbres y Semillas",
            "02-14-01": "Anchoas",
            "02-14-02": "Anchoas",
            "02-14-03": "Atún",
            "02-14-04": "Caballa",
            "02-14-05": "Frutas",
            "02-14-06": "Legumbres",
            "02-14-07": "Paté / Picadillo",
            "02-14-08": "Sardinas",
            "02-14-09": "Tomates / Salsas",
            "02-14-10": "Verduras",
            "02-14-11": "Otras Conservas",
            "02-15-01": "Azúcar",
            "02-15-02": "Cacao en Polvo / Chocolate para Taza",
            "02-15-03": "Café en Cápsulas",
            "02-15-04": "Café Instantáneo",
            "02-15-05": "Café Molido",
            "02-15-06": "Cereales / Avena",
            "02-15-07": "Dulce de Leche",
            "02-15-08": "Edulcorante",
            "02-15-09": "Filtros de Café",
            "02-15-10": "Galletitas de Arroz",
            "02-15-11": "Galletitas Dulces",
            "02-15-12": "Galletitas Saladas",
            "02-15-13": "Leche en Polvo",
            "02-15-14": "Mate Cocido",
            "02-15-15": "Mermeladas y Jaleas",
            "02-15-16": "Miel",
            "02-15-17": "Té",
            "02-15-18": "Yerba",
            "02-16-01": "Frutas Secas y Disecadas",
            "02-17-01": "Alfajores",
            "02-17-02": "Barras de Cereal",
            "02-17-03": "Bocaditos y Bombones",
            "02-17-04": "Caramelos, Chupetines y Chicles",
            "02-17-05": "Chocolates y Tabletas",
            "02-17-06": "Turrones y Confituras",
            "02-17-07": "Otras Golosinas",
            "02-18-01": "Almidón de Maíz",
            "02-18-02": "Fideos Guiseros y de Sopa",
            "02-18-03": "Fideos Largos",
            "02-18-04": "Harina de Maíz",
            "02-18-05": "Harina de Trigo",
            "02-18-06": "Pastas Rellenas",
            "02-18-07": "Sémola",
            "02-18-08": "Otras Pastas / Harinas",
            "02-19-01": "Budines / Bizcochuelos / Piononos",
            "02-19-02": "Hamburguesas y Panchos",
            "02-19-03": "Pan de Molde",
            "02-19-04": "Pan Francés",
            "02-19-05": "Pan Rallado y Rebozadores",
            "02-19-06": "Tostadas",
            "02-19-07": "Otros Productos Panificados",
            "02-20-01": "Bizcochuelos, Brownies y Tortas",
            "02-20-02": "Gelatinas",
            "02-20-03": "Mousse y Helados",
            "02-20-04": "Postres y Flanes",
            "02-20-05": "Premezclas",
            "02-20-06": "Repostería",
            "02-21-01": "Chizitos",
            "02-21-02": "Maní",
            "02-21-03": "Palitos",
            "02-21-04": "Papas fritas",
            "02-21-05": "Otros Snacks",
            "02-22-01": "Puré Instantáneo",
            "02-22-02": "Sopas, Caldos y Saborizadores",
            "02-23-01": "Suplemento Nutricional",
            "03-24-01": "Leche Infantil",
            "03-24-02": "Otros Productos para la Alimentación del Bebé",
            "03-25-01": "Aceites / Cremas / Jabones",
            "03-25-02": "Chupetes y Mamaderas",
            "03-25-03": "Pañales",
            "03-25-04": "Talco / Oleo Calcáreo",
            "03-25-05": "Toallitas Húmedas",
            "04-26-01": "Americano",
            "04-26-02": "Fernet",
            "04-26-03": "Otros Aperitivos",
            "04-27-01": "Bebidas Blancas",
            "04-27-02": "Licores",
            "04-27-03": "Whisky",
            "04-28-01": "En Botella",
            "04-28-02": "En Lata",
            "04-29-01": "Champagne",
            "04-29-02": "Frizantes",
            "04-30-01": "Vinos Blancos",
            "04-30-02": "Vinos Rosados",
            "04-30-03": "Vinos Tintos",
            "04-31-01": "Sidras",
            "04-31-02": "Otras Bebidas con Alcohol",
            "05-32-01": "Amargos",
            "05-33-01": "Gasificadas",
            "05-33-02": "Sin Gas",
            "05-34-01": "Gasificadas",
            "05-34-02": "Sin Gas",
            "05-35-01": "Cervezas y Sidras sin Alcohol",
            "05-36-01": "Cola",
            "05-36-02": "Lima Limón",
            "05-36-03": "Naranja",
            "05-36-04": "Pomelo",
            "05-36-05": "Tónica",
            "05-36-06": "Otras",
            "05-37-01": "",
            "05-38-01": "Concentrados",
            "05-38-02": "En Polvo",
            "05-38-03": "Listos",
            "05-39-01": "Soda",
            "06-40-01": "Carne de Cerdo",
            "06-40-02": "Carne Vacuna",
            "06-40-03": "Hígado y Achuras",
            "06-40-04": "Pollo",
            "06-40-05": "Otras Carnes",
            "06-41-01": "Pescados y Mariscos",
            "06-42-01": "Dulce de Batata / Membrillo",
            "06-42-02": "Fiambres",
            "06-42-03": "Salchichas / Embutidos",
            "06-42-04": "Otros Productos de Fiambrería",
            "06-43-01": "Ananá",
            "06-43-02": "Banana",
            "06-43-03": "Ciruela",
            "06-43-04": "Durazno",
            "06-43-05": "Kiwi",
            "06-43-06": "Limón",
            "06-43-07": "Mandarina",
            "06-43-08": "Manzana",
            "06-43-09": "Naranja",
            "06-43-10": "Pera",
            "06-43-11": "Pomelo",
            "06-43-12": "Uvas",
            "06-43-13": "Otras Frutas",
            "06-44-01": "Huevos",
            "06-45-01": "Crema de Leche",
            "06-45-02": "Leche Chocolatada",
            "06-45-03": "Leche Condensada",
            "06-45-04": "Leche Fluida Descremada",
            "06-45-05": "Leche Fluida Entera",
            "06-45-06": "Mantecas y Margarinas",
            "06-45-07": "Postres y Flanes",
            "06-45-08": "Quesos Blandos",
            "06-45-09": "Quesos Duros",
            "06-45-10": "Quesos Rallados",
            "06-45-11": "Quesos Semiduros",
            "06-45-12": "Quesos Untables",
            "06-45-13": "Ricota",
            "06-45-14": "Yogur Bebible",
            "06-45-15": "Yogur con Cereales / Frutas",
            "06-45-16": "Yogur Firme / Batido",
            "06-45-17": "Otras Leches Fluidas",
            "06-45-18": "Otros Quesos",
            "06-46-01": "Levaduras y Grasas",
            "06-47-01": "Fideos",
            "06-47-02": "Ñoquis",
            "06-47-03": "Pastas Rellenas",
            "06-47-04": "Tapas para Empanadas",
            "06-47-05": "Tapas para Tartas",
            "06-48-01": "Acelga",
            "06-48-02": "Ají",
            "06-48-03": "Ajo",
            "06-48-04": "Apio",
            "06-48-05": "Hinojo",
            "06-48-06": "Batata",
            "06-48-07": "Berenjenas",
            "06-48-08": "Cebolla Común",
            "06-48-09": "Cebolla de Verdeo, puerro",
            "06-48-10": "Coliflor, Brócoli",
            "06-48-11": "Lechuga",
            "06-48-12": "Palta",
            "06-48-13": "Papa Blanca",
            "06-48-14": "Papa Negra",
            "06-48-15": "Radicheta, Radicha, Rúcula",
            "06-48-16": "Repollo",
            "06-48-17": "Tomate Redondo",
            "06-48-18": "Zanahoria",
            "06-48-19": "Zapallitos Frescos",
            "06-48-20": "Zapallo",
            "06-48-21": "Otras Verduras",
            "07-49-01": "Baldes",
            "07-49-02": "Broches y Ganchos",
            "07-49-03": "Escobas / Escobillones / Palos / Cabos",
            "07-49-04": "Secadores y Cepillos",
            "07-49-05": "Esponjas y Guantes",
            "07-49-06": "Paños Multiuso / Trapos",
            "07-49-07": "Otros Accesorios de Limpieza",
            "07-50-01": "Absorbe Humedad",
            "07-50-02": "Aromatizantes",
            "07-50-03": "Desinfectantes",
            "07-50-04": "Desodorantes",
            "07-51-01": "Cucarachas y Hormigas",
            "07-51-02": "Moscas y Mosquitos",
            "07-51-03": "Polillas, Pulgas y Garrapatas",
            "07-51-04": "Repelentes",
            "07-51-05": "Roedores",
            "07-52-01": "Lavandina en Gel",
            "07-52-02": "Lavandina Líquida",
            "07-53-01": "Bolsas",
            "07-53-02": "Detergentes y Lavavajillas",
            "07-53-03": "Escarbadientes",
            "07-53-04": "Fósforos y Encendedores",
            "07-53-05": "Limpiadores",
            "07-53-06": "Limpiavidrios",
            "07-53-07": "Velas",
            "07-54-01": "Aprestos y Blanqueadores",
            "07-54-02": "Cepillos para la Ropa",
            "07-54-03": "Jabón en Pan",
            "07-54-04": "Jabón en Polvo",
            "07-54-05": "Jabón Líquido",
            "07-54-06": "Perfume para la Ropa",
            "07-54-07": "Quitamanchas",
            "07-54-08": "Suavizantes",
            "07-55-01": "Limpiadores / Desinfectantes Cremosos",
            "07-55-02": "Limpiadores / Desinfectantes en Aerosol",
            "07-55-03": "Limpiadores / Desinfectantes en Gel",
            "07-55-04": "Limpiadores / Desinfectantes Líquidos",
            "07-55-05": "Pastillas y Bloques",
            "07-56-01": "Brillos y Revividores",
            "07-56-02": "Limpiadores",
            "07-56-03": "Pomadas para el Calzado",
            "07-57-01": "Ceras y Autobrillos",
            "07-57-02": "Limpiadores de Pisos",
            "07-57-03": "Lustramuebles",
            "07-57-04": "Otros Productos para Limpiar Pisos y Muebles",
            "07-58-01": "Films",
            "07-58-02": "Pañuelos Descartables",
            "07-58-03": "Papel de Aluminio",
            "07-58-04": "Papel Higiénico",
            "07-58-05": "Rollos de Cocina",
            "07-58-06": "Servilletas",
            "08-59-01": "Alimentos para Gatos",
            "08-59-02": "Alimentos para Perros",
            "08-59-03": "Productos de Higiene para Mascotas",
            "08-59-04": "Otros Productos para Mascotas",
            "09-60-01": "Acondicionadores",
            "09-60-02": "Fijadores",
            "09-60-03": "Productos para Combatir la Pediculosis",
            "09-60-04": "Reparación y Tratamiento",
            "09-60-05": "Shampoo",
            "09-60-06": "Tintura",
            "09-61-01": "Algodones e Hisopos",
            "09-61-02": "Cepillos y Esponjas",
            "09-61-03": "Cremas de Manos y Corporales",
            "09-61-04": "Desodorantes Infantiles",
            "09-61-05": "Desodorante para el Hombre",
            "09-61-06": "Desodorante para la Mujer",
            "09-61-07": "Jabones de Tocador / Glicerina",
            "09-61-08": "Jabones Líquidos",
            "09-61-09": "Perfumes",
            "09-61-10": "Productos de Depilación",
            "09-61-11": "Protectores Diarios",
            "09-61-12": "Protectores Solares y Post Solares",
            "09-61-13": "Quitaesmaltes",
            "09-61-14": "Talcos",
            "09-61-15": "Tampones",
            "09-61-16": "Toallas Higiénicas",
            "09-61-17": "Otros Productos para el Cuidado Corporal",
            "09-62-01": "Cremas Antiacné",
            "09-62-02": "Cremas Antiarrugas",
            "09-62-03": "Cremas Hidratantes / Humectantes",
            "09-62-04": "Productos de Afeitar/ para después de Afeitarse",
            "09-62-05": "Desmaquillantes",
            "09-62-06": "Otros Productos para el Cuidado Facial",
            "09-63-01": "Accesorios Dentales",
            "09-63-02": "Cepillos Dentales",
            "09-63-03": "Enjuagues Bucales",
            "09-63-04": "Pastas Dentales",
            "09-64-01": "Alcohol",
            "09-64-02": "Antisépticos",
            "09-64-03": "Apósitos Protectores",
            "09-64-04": "Preservativos",
            "09-64-05": "Protectores Mamarios",
            "09-64-06": "Otros",
            "09-65-01": "Pañales para Adultos",
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
