# -*- coding: utf-8 -*-
import datetime
from pathlib import Path
from scrapy.exporters import CsvItemExporter
from scrapy import signals
from pydispatch import dispatcher

from scrapy.exceptions import DropItem
from preciosclaros.items import PrecioItem


class DuplicatesPipeline(object):
    def __init__(self):
        self.ids_seen = set()

    def process_item(self, item, spider):
        if isinstance(item, PrecioItem):
            return item

        id_ = item.get("id")
        # esto puede ocupar mucha memoria \o/
        if id_ and id_ in self.ids_seen:
            raise DropItem(f"Duplicate item found: {id_}")
        else:
            self.ids_seen.add(id_)
            return item


def item_type(item):
    return type(item).__name__.replace("Item", "").lower()  # TeamItem => team


class MultiCSVItemPipeline:

    items = {"sucursal", "producto", "precio"}

    def __init__(self):
        dispatcher.connect(self.spider_opened, signal=signals.spider_opened)
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

    def spider_opened(self, spider):
        if not spider.exportar:
            return
        today = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        data = Path("data/")
        data.mkdir(exist_ok=True)

        self.files = {
            name: (data / f"{name}-{spider.porcion}-{spider.total_spiders}-{today}.csv").open("w+b") for name in MultiCSVItemPipeline.items
        }
        self.exporters = {name: CsvItemExporter(self.files[name]) for name in MultiCSVItemPipeline.items}
        for e in self.exporters.values():
            e.start_exporting()

    def spider_closed(self, spider):
        if not spider.exportar:
            return

        for e in self.exporters.values():
            e.finish_exporting()
        for f in self.files.values():
            f.close()

    def process_item(self, item, spider):
        if spider.exportar:
            what = item_type(item)
            self.exporters[what].export_item(item)
        return item
