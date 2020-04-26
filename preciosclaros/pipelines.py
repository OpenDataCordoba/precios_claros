# -*- coding: utf-8 -*-
import datetime
from pathlib import Path
from scrapy.exporters import CsvItemExporter
from scrapy import signals
from pydispatch import dispatcher

from scrapy.exceptions import DropItem
from preciosclaros.items import PrecioItem, ProductoCategorizadoItem


class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if isinstance(item, PrecioItem):
            # los precios los queremos siempre
            return item

        id_ = item.get("id")
        # esto puede ocupar mucha memoria \o/
        if id_ and id_ in self.ids_seen:
            raise DropItem(f"producto ya bajado")
        else:
            self.ids_seen.add(id_)
            return item


def item_type(item):
    if isinstance(item, ProductoCategorizadoItem):
        return "producto_cat"
    return type(item).__name__.replace("Item", "").lower()  # TeamItem => team


class MultiCSVItemPipeline:

    items = {"sucursal", "producto", "precio"}

    def __init__(self):
        self.files = {}
        self.exporters = {}
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

    def open_exporter(self, spider, item_type):
        """
        Inicializa el archivo y el exportador de salida
        Se invoca ante el primer
        """
        today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        data = Path("data/")
        data.mkdir(exist_ok=True)

        self.files[item_type] = (data / f"{item_type}-{spider.porcion}-{spider.total_spiders}-{today}.csv").open("w+b")
        self.exporters[item_type] = CsvItemExporter(self.files[item_type])
        self.exporters[item_type].start_exporting()

    def spider_closed(self, spider):
        for e in self.exporters.values():
            e.finish_exporting()
        for f in self.files.values():
            f.close()

    def process_item(self, item, spider):
        if spider.exportar:
            self.export_item(item, spider)
        return item

    def export_item(self, item, spider):
        name = item_type(item)
        if name not in self.files:
            self.open_exporter(spider, name)
        self.exporters[name].export_item(item)
