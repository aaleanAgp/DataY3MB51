"""
descarga_preempaque.py — Descarga de órdenes en estado PREEMPAQUE desde el servidor Comercial.
Uso: python scripts/descarga_preempaque.py
"""

import os
import time
import json
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
SERVER   = os.environ.get("SQL_COMERCIAL_SERVER")
DATABASE = os.environ.get("SQL_COMERCIAL_DATABASE", "Comercial")
USERNAME = os.environ.get("SQL_COMERCIAL_USERNAME")
PASSWORD = os.environ.get("SQL_COMERCIAL_PASSWORD")

# Si no hay credenciales comerciales específicas, podemos intentar usar las de MB51 si es el mismo servidor
if not SERVER:
    SERVER = os.environ.get("SQL_SERVER")
    USERNAME = os.environ.get("SQL_USERNAME")
    PASSWORD = os.environ.get("SQL_PASSWORD")

OUTPUT_PATH  = Path(__file__).parent.parent / "data" / "preempaque.parquet"
METADATA_PATH = Path(__file__).parent.parent / "data" / "preempaque_metadata.json"

QUERY = """
SELECT [ORDEN]
      ,[CLV_MODEL]
      ,[TXT_MATERIAL]
      ,[DATE_NOTIF]
      ,[ANULADO]
      ,[ZFER]
FROM [Comercial].[dbo].[BI_TAB_FIN_TURNO_SAP]
WHERE [CLV_MODEL] = 'PREEMPQ'
ORDER BY [DATE_NOTIF] DESC
"""

# ---------------------------------------------------------------------------
# Detección automática de driver ODBC (Reutilizado de descarga.py)
# ---------------------------------------------------------------------------
def detect_odbc_driver() -> str:
    drivers = pyodbc.drivers()
    candidates = [d for d in drivers if "ODBC Driver" in d and "SQL Server" in d]
    if not candidates:
        raise RuntimeError("No se encontró ningún driver ODBC para SQL Server.")
    candidates.sort(key=lambda x: int("".join(filter(str.isdigit, x.split("ODBC Driver")[1][:3]))), reverse=True)
    return candidates[0]

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

def download_preempaque():
    if not (SERVER and USERNAME and PASSWORD):
        print("[ERROR] Faltan credenciales comerciales en el archivo .env.")
        print("Asegúrate de configurar SQL_COMERCIAL_SERVER, SQL_COMERCIAL_USERNAME y SQL_COMERCIAL_PASSWORD.")
        return

    driver = detect_odbc_driver()
    print(f"[INFO] Conectando a {SERVER}/{DATABASE} (Comercial) ...")
    
    conn = get_connection(driver)
    
    print("[INFO] Ejecutando consulta de preempaque...")
    start_time = time.time()
    
    # Cargamos directamente con pandas ya que son ~300k filas
    df = pd.read_sql_query(QUERY, conn)
    
    # Asegurarnos que ORDEN sea string y tenga ceros a la izquierda (12 pos if needed)
    df['ORDEN'] = df['ORDEN'].astype(str).str.strip().str.zfill(12)
    
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_PATH, compression="snappy", index=False)
    
    elapsed = time.time() - start_time
    file_size_mb = OUTPUT_PATH.stat().st_size / (1024 ** 2)
    
    metadata = {
        "fecha_descarga": datetime.now().isoformat(),
        "servidor": SERVER,
        "base_de_datos": DATABASE,
        "filas_descargadas": len(df),
        "tiempo_segundos": round(elapsed, 2),
        "tamanio_parquet_mb": round(file_size_mb, 2),
        "archivo_parquet": str(OUTPUT_PATH),
    }

    with open(METADATA_PATH, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    conn.close()

    print(f"\n[OK] Descarga completa de Preempaque.")
    print(f"     Filas:   {len(df):,}")
    print(f"     Tiempo:  {elapsed:.1f}s")
    print(f"     Tamaño:  {file_size_mb:.1f} MB")
    print(f"     Archivo: {OUTPUT_PATH}")

if __name__ == "__main__":
    download_preempaque()
