# -*- coding: utf-8 -*-
import scrapy


class SucursalItem(scrapy.Item):
    id = scrapy.Field()
    sucursalTipo = scrapy.Field()
    direccion = scrapy.Field()
    provincia = scrapy.Field()
    banderaId = scrapy.Field()
    localidad = scrapy.Field()
    banderaDescripcion = scrapy.Field()
    comercioRazonSocial = scrapy.Field()
    lat = scrapy.Field()
    lng = scrapy.Field()
    sucursalNombre = scrapy.Field()
    comercioId = scrapy.Field()
    sucursalId = scrapy.Field()


class ProductoItem(scrapy.Item):
    id = scrapy.Field()
    nombre = scrapy.Field()
    presentacion = scrapy.Field()
    marca = scrapy.Field()


class ProductoCategorizadoItem(scrapy.Item):
    id = scrapy.Field()
    nombre = scrapy.Field()
    presentacion = scrapy.Field()
    marca = scrapy.Field()
    categoria1 = scrapy.Field()
    categoria2 = scrapy.Field()
    categoria3 = scrapy.Field()


class PrecioItem(scrapy.Item):
    sucursal_id = scrapy.Field()
    producto_id = scrapy.Field()
    precio = scrapy.Field()
    precio_max = scrapy.Field()
    precio_min = scrapy.Field()
    fecha_relevamiento = scrapy.Field(serializer=str)
