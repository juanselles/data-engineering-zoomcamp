# Module 2 Homework: Workflow Orchestration (Kestra & Postgres)

Este repositorio contiene la solución para el Homework del Módulo 2 del Data Engineering Zoomcamp 2026. Se ha utilizado **Kestra** corriendo en Docker para la orquestación y **Postgres** como base de datos destino.

## 🛠️ Tecnologías

* **Orquestador:** Kestra (Docker Image)
* **Base de Datos:** Postgres 16+
* **Lenguaje:** YAML (Flows) y SQL

## 📂 Estructura del Proyecto

A continuación se presentan los scripts utilizados para la ingesta y transformación de los datos de *NYC TLC Data*.

### 1. Kestra Flow (`02_postgres_taxi.yaml`)
Este es el flujo principal que descarga, descomprime y carga los datos.
**Nota:** Se utiliza `FileDecompress` con `compression: "GZIP"` para manejar correctamente los archivos `.gz` individuales y se concatena la URL para evitar errores de caracteres especiales.

```yaml
id: 02_homework_zoomcamp
namespace: zoomcamp
description: Flow para cargar datos de Taxis (Yellow/Green) a Postgres

inputs:
  - id: taxi
    type: SELECT
    displayName: Select Taxi Type
    values: ["yellow", "green"]
    defaults: "yellow"
  - id: year
    type: STRING
    displayName: Year
    defaults: "2020"
  - id: month
    type: STRING
    displayName: Month (e.g. 01, 04, 12)
    defaults: "12"

tasks:
  - id: download_file
    type: io.kestra.plugin.core.http.Download
    uri: "{{ '[https://github.com/DataTalksClub/nyc-tlc-data/releases/download/](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/)' ~ inputs.taxi ~ '/' ~ inputs.taxi ~ '_tripdata_' ~ inputs.year ~ '-' ~ inputs.month ~ '.csv.gz' }}"

  - id: decompress_file
    type: io.kestra.plugin.compress.FileDecompress
    compression: "GZIP"
    from: "{{ outputs.download_file.uri }}"

  - id: load_to_postgres
    type: io.kestra.plugin.jdbc.postgresql.CopyIn
    url: jdbc:postgresql://pgdatabase:5432/ny_taxi
    username: root
    password: root
    format: CSV
    header: true
    table: "{{ inputs.taxi ~ '_tripdata' }}"
    from: "{{ outputs.decompress_file.uri }}"
```

## ✅ Soluciones al Quiz

### Question 3. Rows for Yellow Taxi data 2020
How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?
* **Respuesta:** `24,648,499`
* *Consulta SQL:*
```sql
SELECT COUNT(*) 
FROM yellow_tripdata 
WHERE tpep_pickup_datetime >= '2020-01-01' AND tpep_pickup_datetime < '2021-01-01';
```

### Question 4. Rows for Green Taxi data 2020
How many rows are there for the Green Taxi data for all CSV files in the year 2020?
* **Respuesta:** `1,734,051`
* *Consulta SQL:*
```sql
SELECT COUNT(*) 
FROM green_tripdata 
WHERE lpep_pickup_datetime >= '2020-01-01' AND lpep_pickup_datetime < '2021-01-01';
```

### Question 5. Rows for Yellow Taxi March 2021
How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
* **Respuesta:** `1,925,152
* *Consulta SQL:*
```sql
SELECT COUNT(*) 
FROM yellow_tripdata 
WHERE tpep_pickup_datetime >= '2021-03-01' AND tpep_pickup_datetime < '2021-04-01';
```

### Question 6. Timezone configuration
How would you configure the timezone to New York in a Schedule trigger?

Respuesta: Add a timezone property set to America/New_York in the Schedule trigger configuration.

Explicación: Kestra requiere el formato IANA (Continente/Ciudad) para gestionar correctamente los cambios de hora (Daylight Saving Time), algo que UTC-5 o EST no hacen automáticamente.
