# Scraper de preciosclaros.gob.ar

Un crawler (basado en Scrapy) para descargar todos los productos, precios y
sucursales listadas en este portal de precios al consumidor.

## Ejecución local

Por la gran cantidad de items que se descargan, el proceso demora varias
horas en completarse. Lo que se hace es correr diferentes
"chunks" o porciones de las sucursales disponibles.

Para una ejecución manual que genera archivos CSV:

```
$ scrapy crawl preciosclaros -a porcion=<PARTE>/<TOTAL_PARTES> -a exportar=1 --loglevel=INFO
```

Por ejemplo, si `porcion` es `1/7` (default), bajará el primer séptimo del total de sucursales, si es 2/3 bajará el segundo tercio, etc.

El parámetro `exportar` es recibido por el spider y permite que un pipeline
escriba cada item a un CSV en
`data/<tipo_item>-<porcion>-<cantidad_porciones-<marca_de_tiempo_inicial>.csv`
donde tipo de item es sucursal, producto o precio.

Tambien se pueden desactivar la descarga de productos con `-a productos=0` y
precios con `-a precios=0`

Por ejemplo,

```
$ scrapy crawl preciosclaros -a porcion=1 -a exportar=1 -a productos=0 -a precios=0
```

Bajará todas las sucursales en un único CSV.


Los argumentos `-a max_sucursales_por_cadena=<N>` y opcionalmente
`-a max_sucursales_criterio=<criterio>` donde N es un entero
positivo (0, el default, significa "ilimitado"), y `<criterio>`
puede ser `"localidad"` o `"provincia"`
permite limitar el scraping de N sucursales de una cadena en ese territorio.
Por ejemplo `-a max_sucursales_por_cadena=1 -a max_sucursales_criterio=provincia` bajará los precios de sólo una sucursal
testigo por cadena (Jumbo, Disco, Walmart, etc.) por cada provincia. Esto reduce el volumen de datos scrapeados un 92%.


## Ejecutar en la nube


Alternativamente, se puede ejecutar en la plataforma [Scrapy Cloud](https://scrapinghub.com/scrapy-cloud/).


## Notas

- El código origianal migrado del proyecto [Preciosa](https://github.com/mgaitan/preciosa).

- Esta información deberia ser provista por la Secretaria de Comercio