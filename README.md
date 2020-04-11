# Scraper de preciosclaros.gob.ar

Un crawler (basado en Scrapy) para descargar todos los productos, precios y
sucursales listadas en este portal de precios al consumidor.

## Cómo ejecutar

Por la gran cantidad de items que se descargan, el proceso demora varias
horas en completarse. Lo que se hace es correr diferentes
"chunks" o porciones de las sucursales disponibles.

Para una ejecución manual que genera archivos CSV:

```
$ scrapy crawl preciosclaros -a porcion=<PARTE>/<TOTAL_PARTES> -a exportar=1
```

Por ejemplo, si `porcion` es 1/7 (default), bajará el primer séptimo del total
de sucursales, si es 2/3 bajará el segundo tercio, etc.

El parámetro `exportar` es recibido por el spider y permite que un pipeline
escriba cada item a un CSV en
`data/<tipo_item>-<porcion>-<cantidad_porciones-<marca_de_tiempo_inicial>.csv`

Tambien se pueden desactivar la descarga de productos con `-a productos=0` y
precios con `-a precios=0`

Por ejemplo,

```
$ scrapy crawl preciosclaros -a porcion=1 -a exportar=1 -a productos=0 -a precios=0
```

Bajará todas las sucursales en un único CSV.


Alternativamente, se puede ejecutar en la plataforma [Scrapy Cloud](https://scrapinghub.com/scrapy-cloud/).


## Notas

- El código origianal migrado del proyecto [Preciosa](https://github.com/mgaitan/preciosa).

- Esta información deberia ser provista por la Secretaria de Comercio