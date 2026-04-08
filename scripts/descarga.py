"""
descarga.py — Descarga MB51 desde Azure SQL Server a Parquet local.
Uso: python scripts/descarga.py
"""

import os
import time
import json
import struct
from datetime import datetime
from pathlib import Path

import pyodbc
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
SERVER   = os.environ["SQL_SERVER"]
DATABASE = os.environ["SQL_DATABASE"]
USERNAME = os.environ["SQL_USERNAME"]
PASSWORD = os.environ["SQL_PASSWORD"]

CHUNK_SIZE   = 100_000
OUTPUT_PATH  = Path(__file__).parent.parent / "data" / "mb51.parquet"
METADATA_PATH = Path(__file__).parent.parent / "data" / "descarga_metadata.json"

QUERY = """
SELECT [ID],[MANDT],[DOC_MATERIAL],[POSICION],[REGISTRADO_EL],[HORA],
       [FECHA_CONTA],[OPERACION],[MATERIAL],[TEXTO_MATE],[ALMACEN],
       [LOTE],[ORDEN],[CLASE_MOV],[USUARIO],[CANTIDAD],[UNIDAD],
       [IMPORTE],[MONEDA],[CENTRO],[ProcessDate]
FROM [dbo].[ODATA_MB51_TEST]
"""


# ---------------------------------------------------------------------------
# Detección automática de driver ODBC
# ---------------------------------------------------------------------------
def detect_odbc_driver() -> str:
    """Devuelve el driver ODBC de SQL Server disponible con mayor versión."""
    drivers = pyodbc.drivers()
    candidates = [d for d in drivers if "ODBC Driver" in d and "SQL Server" in d]
    if not candidates:
        raise RuntimeError(
            "No se encontró ningún driver ODBC para SQL Server.\n"
            "Instala 'ODBC Driver 17 for SQL Server' o 'ODBC Driver 18 for SQL Server'.\n"
            "Descarga: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server"
        )
    # Ordenar por número de versión descendente (18 > 17 > ...)
    candidates.sort(key=lambda x: int("".join(filter(str.isdigit, x.split("ODBC Driver")[1][:3]))), reverse=True)
    selected = candidates[0]
    print(f"[ODBC] Driver detectado: {selected}")
    return selected


# ---------------------------------------------------------------------------
# Conexión
# ---------------------------------------------------------------------------
def get_connection(driver: str) -> pyodbc.Connection:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={USERNAME};"
        f"PWD={PASSWORD};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str, timeout=60)


# ---------------------------------------------------------------------------
# Descarga en chunks → Parquet
# ---------------------------------------------------------------------------
def download_to_parquet():
    driver = detect_odbc_driver()

    print(f"[INFO] Conectando a {SERVER}/{DATABASE} ...")
    conn = get_connection(driver)
    cursor = conn.cursor()

    # Contar filas para la barra de progreso
    print("[INFO] Contando filas totales ...")
    cursor.execute("SELECT COUNT(*) FROM [dbo].[ODATA_MB51_TEST]")
    total_rows = cursor.fetchone()[0]
    print(f"[INFO] Total de filas a descargar: {total_rows:,}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    writer = None
    schema = None
    rows_downloaded = 0
    start_time = time.time()

    cursor.execute(QUERY)
    columns = [col[0] for col in cursor.description]

    with tqdm(total=total_rows, unit="filas", desc="Descargando") as pbar:
        while True:
            chunk = cursor.fetchmany(CHUNK_SIZE)
            if not chunk:
                break

            df = pd.DataFrame.from_records(chunk, columns=columns)

            # Inferir schema en el primer chunk para mantenerlo consistente
            table = pa.Table.from_pandas(df, preserve_index=False)
            if writer is None:
                schema = table.schema
                writer = pq.ParquetWriter(
                    OUTPUT_PATH,
                    schema,
                    compression="snappy",
                )

            writer.write_table(table)
            rows_downloaded += len(df)
            pbar.update(len(df))

    if writer:
        writer.close()

    elapsed = time.time() - start_time
    file_size_mb = OUTPUT_PATH.stat().st_size / (1024 ** 2)

    metadata = {
        "fecha_descarga": datetime.now().isoformat(),
        "servidor": SERVER,
        "base_de_datos": DATABASE,
        "filas_descargadas": rows_downloaded,
        "tiempo_segundos": round(elapsed, 2),
        "tamanio_parquet_mb": round(file_size_mb, 2),
        "driver_odbc": driver,
        "chunk_size": CHUNK_SIZE,
        "archivo_parquet": str(OUTPUT_PATH),
    }

    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    conn.close()

    print(f"\n[OK] Descarga completa.")
    print(f"     Filas:   {rows_downloaded:,}")
    print(f"     Tiempo:  {elapsed:.1f}s  ({elapsed/60:.1f} min)")
    print(f"     Tamaño:  {file_size_mb:.1f} MB")
    print(f"     Archivo: {OUTPUT_PATH}")
    print(f"     Meta:    {METADATA_PATH}")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    try:
        download_to_parquet()
    except Exception as e:
        print(f"\n[ERROR] {e}")
        raise
