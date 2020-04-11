# Scraper de preciosclaros.gob.ar

Un crawler (basado en Scrapy) para descargar todos los productos, precios y
sucursales listadas en este portal de precios al consumidor.

## Cómo ejecutar

Por la gran cantidad de items que se descargan, el proceso demora varias
horas en completarse. Lo que se hace es correr diferentes
"chunks" o porciones de las sucursales disponibles.

Para una ejecución manual que genera archivos CSV:

```
$ scrapy crawl preciosclaros -a porcion=<PORCION> -a exportar=1
```

Siendo `<PORCION>` un valor entero entre 0 y 6. El valor por defecto es 0.

El parámetro `exportar` es recibido por el spider y permite que un pipeline
escriba cada item a un CSV en
`data/<tipo_item>-<porcion>-<marca_de_tiempo_inicial>.csv`


Alternativamente, se puede ejecutar en la plataforma [Scrapy Cloud](https://scrapinghub.com/scrapy-cloud/).


## Notas

- El código origianal migrado del proyecto [Preciosa](https://github.com/mgaitan/preciosa).

- Esta información deberia ser provista por la Secretaria de Comercio