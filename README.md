# Scraper de preciosclaros.gob.ar

Un crawler (basado en Scrapy) para descargar todos los productos, precios y
sucursales listadas en este portal de precios al consumidor.

Intentamos mantener datos históricos de precios acá https://www.kaggle.com/tinnqn/precios-claros-precios-de-argentina


## Instalación

En un virtualenv con Python 3.6+

```
$ pip install -r requirements.txt
$ pip install -e .
```

## Ejecución local

Por la gran cantidad de items que se descargan, el proceso de un scrapeo completo
demora varias horas en completarse. Por ello el spider soporta diferentes
"chunks" o porciones de las sucursales disponibles.

Para una ejecución manual que genera archivos CSV:

```
$ scrapy crawl preciosclaros -a porcion=<PARTE>/<TOTAL_PARTES> -a exportar=1 --loglevel=INFO
```

Por ejemplo, si `porcion` es `1/7` (default), bajará el primer séptimo del total de sucursales, si es 2/3 bajará el segundo tercio, etc.

El parámetro `exportar` permite que cada item sea exportar a un CSV en
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

Una vez registrado en la plataforma, desde el directorio donde está el código

```
$ shub deploy <PROJECT_ID>
```

La primera vez se solicitará ingresar el `api-token` del proyecto. Este deploy debe hacerse por cada nueva version (o modificación local).

Luego, para agendar la ejecución de un job,

```
$ shub deploy preciosclaros <args>...
```

donde args son los mismos argumentos `-a arg=val` del spider.



## Notas

- El código origianal migrado del proyecto [Preciosa](https://github.com/mgaitan/preciosa).

- Esta información deberia ser provista por la Secretaria de Comercio
