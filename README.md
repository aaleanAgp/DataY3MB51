# MB51 — Análisis de movimientos de material SAP

Descarga y análisis local de movimientos de material (tabla `ODATA_MB51_TEST`) desde Azure SQL Server.
Almacenamiento en Parquet · Consultas con DuckDB · Visualizaciones con Matplotlib/Seaborn.

---

## Estructura

```
mb51_analisis/
├── data/                   # mb51.parquet + metadatos de descarga
├── notebooks/
│   └── 01_exploracion.ipynb
├── scripts/
│   └── descarga.py
├── outputs/                # gráficos exportados
├── .env.example
├── .gitignore
├── requirements.txt
└── README.md
```

---

## Pasos de instalación

### 1. Requisito previo — Driver ODBC

Necesitas **ODBC Driver 17 o 18 for SQL Server** instalado en Windows.
Si aún no lo tienes, descárgalo desde Microsoft:

- [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server)

Verifica que esté instalado:
```powershell
Get-OdbcDriver | Where-Object { $_.Name -like "*SQL Server*" }
```

### 2. Crear y activar el entorno virtual

```bash
cd mb51_analisis
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar credenciales

```bash
cp .env.example .env
# Edita .env con tus valores reales:
#   SQL_SERVER, SQL_DATABASE, SQL_USERNAME, SQL_PASSWORD
```

---

## Descargar datos

```bash
python scripts/descarga.py
```

El script:
- Detecta automáticamente el driver ODBC disponible (18 > 17)
- Descarga en chunks de 100.000 filas sin cargar todo en RAM
- Guarda `data/mb51.parquet` con compresión snappy
- Genera `data/descarga_metadata.json` con fecha, filas, tiempo y tamaño

Tiempo estimado para ~10M filas: **10–25 minutos** según ancho de banda.

---

## Exploración y análisis

```bash
jupyter notebook notebooks/01_exploracion.ipynb
```

El notebook incluye:
1. Estadísticas básicas (filas, nulos, tipos, rango de fechas)
2. Comparativa 261 vs 201 (movimientos, cantidad, importe)
3. Top 20 materiales por cantidad e importe
4. Evolución temporal diaria y mensual
5. Plantilla SQL para queries ad-hoc con DuckDB

Los gráficos se guardan automáticamente en `outputs/`.

---

## Queries DuckDB de ejemplo

```python
import duckdb
con = duckdb.connect()
con.execute("CREATE VIEW mb51 AS SELECT * FROM read_parquet('data/mb51.parquet')")

# Importe total por material y mes
df = con.execute("""
    SELECT MATERIAL,
           DATE_TRUNC('month', CAST(REGISTRADO_EL AS DATE)) AS mes,
           SUM(ABS(IMPORTE)) AS importe
    FROM mb51
    GROUP BY 1, 2
    ORDER BY 3 DESC
""").df()
```

---

## Dependencias principales

| Paquete | Versión mín. | Uso |
|---------|-------------|-----|
| pyodbc | 4.0.39 | Conexión a Azure SQL Server |
| pandas | 2.1 | Transformación de chunks |
| pyarrow | 14 | Escritura Parquet |
| duckdb | 0.10 | Consultas SQL sobre Parquet |
| tqdm | 4.66 | Barra de progreso |
| python-dotenv | 1.0 | Variables de entorno |
| jupyter | 1.0 | Notebooks |
| matplotlib / seaborn | 3.8 / 0.13 | Visualizaciones |
