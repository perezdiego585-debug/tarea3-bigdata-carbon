#!/usr/bin/env bash
# Descarga el conjunto de datos de la ANM desde el Portal de Datos Abiertos de Colombia.
# La API SODA entrega el CSV con los nombres técnicos de las columnas.
set -e
URL='https://www.datos.gov.co/resource/r85m-vv6c.csv?$limit=2000000'
echo "Descargando ANM - Volumen de explotación de minerales asociados a regalías..."
wget -O regalias_minerales.csv "$URL"
echo "Filas descargadas (incluye encabezado): $(wc -l < regalias_minerales.csv)"
head -3 regalias_minerales.csv
