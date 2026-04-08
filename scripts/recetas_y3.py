"""
recetas_y3.py — Cruce MB51 + Preempaque (ORDEN→ZFER) + BOM colsap.

Flujo de datos:
  1. MB51 parquet → identificar órdenes con Y3 (por lotes desde IM02)
  2. preempaque.parquet → mapeo ORDEN → ZFER (local, ya tiene ZFER)
  3. colsap ZFER_BOM → receta con POSICION, TIPO, ESPESOR, CLAVE_FORMULA

Genera outputs/recetas_y3.xlsx con 3 hojas:
  1. ordenes_zfer  : cada (orden, lote) con su ZFER, ZFOR y consumo
  2. bom_por_zfer  : BOM de cada ZFER involucrado
  3. receta_orden  : receta completa cruzada con consumo real
"""
import os
import warnings
import pyodbc
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
warnings.filterwarnings('ignore', category=UserWarning)

PARQUET         = Path('data/mb51.parquet')
PARQUET_PREEMPQ = Path('data/preempaque.parquet')
OUTPUT          = Path('outputs/recetas_y3.xlsx')


# ── Helpers ────────────────────────────────────────────────────────────────

def parse_sap_num(serie: pd.Series) -> pd.Series:
    s = serie.str.strip()
    negativo = s.str.endswith('-')
    valor = pd.to_numeric(s.str.rstrip('-').str.replace(',', '', regex=False), errors='coerce')
    return valor.where(~negativo, -valor)


def cargar_parquet() -> pd.DataFrame:
    df = pd.read_parquet(PARQUET)
    if 'ORDEN' in df.columns:
        df['ORDEN'] = df['ORDEN'].astype(str).str.strip().str.zfill(12)
    df['CANTIDAD_N'] = parse_sap_num(df['CANTIDAD'])
    df['IMPORTE_N']  = parse_sap_num(df['IMPORTE'])
    df['FECHA']      = pd.to_datetime(df['REGISTRADO_EL'], format='%Y%m%d', errors='coerce')
    return df


def cargar_preempaque() -> pd.DataFrame:
    if not PARQUET_PREEMPQ.exists():
        return pd.DataFrame(columns=['ORDEN', 'ZFER'])
    df = pd.read_parquet(PARQUET_PREEMPQ)
    df['ORDEN'] = df['ORDEN'].astype(str).str.strip().str.zfill(12)
    df['ZFER'] = df['ZFER'].astype(str).str.strip()
    # Solo necesitamos ORDEN y ZFER
    return df[['ORDEN', 'ZFER']].drop_duplicates()


def detect_odbc_driver() -> str:
    drivers = pyodbc.drivers()
    candidates = [d for d in drivers if "ODBC Driver" in d and "SQL Server" in d]
    if not candidates:
        raise RuntimeError("No se encontró ningún driver ODBC para SQL Server.")
    candidates.sort(
        key=lambda x: int("".join(filter(str.isdigit, x.split("ODBC Driver")[1][:3]))),
        reverse=True
    )
    return candidates[0]


def get_colsap_connection(driver: str) -> pyodbc.Connection:
    server   = os.environ["SQL_COLSAP_SERVER"]
    database = os.environ["SQL_COLSAP_DATABASE"]
    username = os.environ["SQL_COLSAP_USERNAME"]
    password = os.environ["SQL_COLSAP_PASSWORD"]

    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        "Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=60;"
    )
    return pyodbc.connect(conn_str, timeout=60)


# ── Consultas colsap ──────────────────────────────────────────────────────

def consultar_zfer_head(conn, materiales: list) -> pd.DataFrame:
    """Obtener ZFOR y descripción de cada ZFER."""
    if not materiales:
        return pd.DataFrame(columns=['MATERIAL', 'ZFOR', 'TEXTO_BREVE_MATERIAL'])

    materiales_int = [int(m) for m in materiales if str(m).strip()]
    if not materiales_int:
        return pd.DataFrame(columns=['MATERIAL', 'ZFOR', 'TEXTO_BREVE_MATERIAL'])

    chunks = []
    chunk_size = 2000
    for i in range(0, len(materiales_int), chunk_size):
        batch = materiales_int[i:i + chunk_size]
        placeholders = ','.join(str(x) for x in batch)
        query = f"""
            SELECT DISTINCT
                CAST(MATERIAL AS VARCHAR(20)) AS MATERIAL,
                ZFOR,
                TEXTO_BREVE_MATERIAL
            FROM ODATA_ZFER_HEAD
            WHERE MATERIAL IN ({placeholders})
        """
        chunk = pd.read_sql_query(query, conn)
        chunks.append(chunk)

    if chunks:
        return pd.concat(chunks, ignore_index=True).drop_duplicates()
    return pd.DataFrame(columns=['MATERIAL', 'ZFOR', 'TEXTO_BREVE_MATERIAL'])


def consultar_zfer_bom(conn, materiales: list) -> pd.DataFrame:
    """Obtener BOM (TIPO_POSICION='K') por ZFER."""
    if not materiales:
        return pd.DataFrame(columns=[
            'MATERIAL', 'POSICION', 'TIPO', 'ESPESOR',
            'CLAVE_FORMULA', 'CANTIDAD'
        ])

    materiales_int = [int(m) for m in materiales if str(m).strip()]
    if not materiales_int:
        return pd.DataFrame(columns=[
            'MATERIAL', 'POSICION', 'TIPO', 'ESPESOR',
            'CLAVE_FORMULA', 'CANTIDAD'
        ])

    print(f'  Consultando BOM para {len(materiales_int):,} ZFER...')
    chunks = []
    chunk_size = 500  # Reducido para evitar timeouts
    total = len(materiales_int)

    for i in range(0, total, chunk_size):
        batch = materiales_int[i:i + chunk_size]
        placeholders = ','.join(str(x) for x in batch)
        query = f"""
            SELECT
                CAST(MATERIAL AS VARCHAR(20)) AS MATERIAL,
                POSICION,
                TIPO,
                ESPESOR,
                CLAVE_FORMULA,
                CANTIDAD,
                CLASE
            FROM ODATA_ZFER_BOM
            WHERE MATERIAL IN ({placeholders})
              AND TIPO_POSICION = 'K'
            ORDER BY MATERIAL, POSICION
        """
        chunk = pd.read_sql_query(query, conn)
        if len(chunk) > 0:
            chunks.append(chunk)
        if (i // chunk_size + 1) % 5 == 0 or (i + chunk_size) >= total:
            print(f'    Progreso: {min(i + chunk_size, total):,}/{total:,} ZFER consultados')

    if chunks:
        return pd.concat(chunks, ignore_index=True)
    return pd.DataFrame(columns=[
        'MATERIAL', 'POSICION', 'TIPO', 'ESPESOR',
        'CLAVE_FORMULA', 'CANTIDAD', 'CLASE'
    ])


# ── Main ───────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('=' * 60)
    print('RECETAS Y3 — Cruce MB51 + Preempaque + BOM colsap')
    print('=' * 60)

    # ── 1. Cargar MB51 e identificar órdenes con Y3 ──────────────────────
    print('\n[1/6] Cargando MB51 parquet...')
    df = cargar_parquet()

    # Lotes Y3 desde IM02 (fuente de verdad)
    mask_im02_y3 = (
        (df['ALMACEN'] == 'IM02') &
        df['TEXTO_MATE'].str.contains('y3', case=False, na=False)
    )
    lotes_y3 = set(df[mask_im02_y3]['LOTE'].unique())
    print(f'  Lotes Y3 desde IM02: {len(lotes_y3):,}')

    # Consumo en almacenes de producción
    mask_consumo = df['CLASE_MOV'].isin(['261', '201']) & df['ALMACEN'].isin(['IM01', 'PP04'])
    mask_directo   = mask_consumo & df['TEXTO_MATE'].str.contains('y3', case=False, na=False)
    mask_indirecto = mask_consumo & df['LOTE'].isin(lotes_y3)

    df_y3 = df[mask_directo | mask_indirecto].copy()
    ordenes_y3 = sorted(df_y3['ORDEN'].unique().tolist())
    print(f'  Órdenes con Y3: {len(ordenes_y3):,}')
    print(f'  Movimientos Y3: {len(df_y3):,}')

    # ── 2. Cargar preempaque (ORDEN → ZFER) ──────────────────────────────
    print('\n[2/6] Cargando preempaque.parquet (ORDEN → ZFER)...')
    df_preempq = cargar_preempaque()
    print(f'  Registros preempaque: {len(df_preempq):,}')
    print(f'  Órdenes únicas: {df_preempq["ORDEN"].nunique():,}')
    print(f'  ZFER únicos: {df_preempq["ZFER"].nunique():,}')

    # ── 3. Identificar ZFER de órdenes Y3 ────────────────────────────────
    print('\n[3/6] Identificando ZFER de órdenes Y3...')
    ordenes_y3_set = set(ordenes_y3)
    df_preempq_y3 = df_preempq[df_preempq['ORDEN'].isin(ordenes_y3_set)].copy()
    zfer_y3 = set(df_preempq_y3['ZFER'].unique())
    print(f'  Órdenes Y3 con ZFER: {df_preempq_y3["ORDEN"].nunique():,}')
    print(f'  ZFER únicos en órdenes Y3: {len(zfer_y3):,}')

    zfer_materiales = sorted([z for z in zfer_y3 if z and z != 'nan'])

    # ── 4. Consultar colsap: ZFER_HEAD + ZFER_BOM ────────────────────────
    print('\n[4/6] Conectando a colsap para BOM...')
    driver = detect_odbc_driver()
    conn = get_colsap_connection(driver)

    print(f'\n[5/6] Consultando ZFER_HEAD (ZFOR)...')
    df_zfer = consultar_zfer_head(conn, zfer_materiales)
    zfer_con_zfor = df_zfer['ZFOR'].notna().sum()
    print(f'  ZFER con ZFOR: {zfer_con_zfor:,}')

    print(f'\n[5/6] Consultando ZFER_BOM (recetas)...')
    df_bom = consultar_zfer_bom(conn, zfer_materiales)
    print(f'  Componentes BOM: {len(df_bom):,}')

    conn.close()
    print('\n  Conexión colsap cerrada.')

    # ── 6. Cruzar datos y generar Excel ──────────────────────────────────
    print('\n[6/6] Cruzando datos y generando Excel...')

    # Consumo por (orden + lote)
    consumo_orden_lote = (
        df_y3.groupby(['ORDEN', 'LOTE'])
        .agg(
            m2_y3        =('CANTIDAD_N', lambda x: x.abs().sum()),
            importe_COP  =('IMPORTE_N',  lambda x: x.abs().sum()),
            fecha_orden  =('FECHA',      'min'),
        )
        .reset_index()
    )

    # Orden + Lote → ZFER via preempaque
    df_con_zfer = consumo_orden_lote.merge(df_preempq_y3, on='ORDEN', how='left')

    # Agregar ZFOR
    df_con_zfer = df_con_zfer.merge(df_zfer, left_on='ZFER', right_on='MATERIAL', how='left')

    # Estado preempaque
    df_con_zfer['ESTADO_PREEMPAQUE'] = 'PREEMPAQUE'
    df_con_zfer.loc[df_con_zfer['ZFER'].isna(), 'ESTADO_PREEMPAQUE'] = 'PENDIENTE'

    # Hoja 1: ordenes_zfer
    df_hoja1 = df_con_zfer[[
        'ORDEN', 'LOTE', 'ZFER', 'TEXTO_BREVE_MATERIAL', 'ZFOR',
        'ESTADO_PREEMPAQUE', 'fecha_orden', 'm2_y3', 'importe_COP'
    ]].rename(columns={
        'TEXTO_BREVE_MATERIAL': 'TEXTO_ZFER',
    }).sort_values(['ORDEN', 'ZFER'])

    # Hoja 2: bom_por_zfer
    df_hoja2 = (
        df_bom.merge(
            df_zfer[['MATERIAL', 'ZFOR', 'TEXTO_BREVE_MATERIAL']],
            on='MATERIAL', how='left'
        )
        .rename(columns={'MATERIAL': 'ZFER_MATERIAL'})
        [[
            'ZFER_MATERIAL', 'TEXTO_BREVE_MATERIAL', 'ZFOR',
            'POSICION', 'TIPO', 'CLASE', 'ESPESOR', 'CLAVE_FORMULA', 'CANTIDAD'
        ]].sort_values(['ZFER_MATERIAL', 'POSICION'])
    )

    # Hoja 3: receta_orden (resumen por orden, no cross join completo)
    # Para cada orden: ZFER + resumen del BOM
    resumen_bom = (
        df_bom.groupby('MATERIAL')
        .agg(
            n_capas     =('POSICION', 'count'),
            tipos_capa  =('TIPO', lambda x: ' | '.join(sorted(x.unique()))),
            clases      =('CLASE', lambda x: ' | '.join(sorted(x.dropna().unique()))),
            espesores   =('ESPESOR', lambda x: ' | '.join([str(e) for e in sorted(x.dropna().unique())])),
            formulas    =('CLAVE_FORMULA', lambda x: ' | '.join(sorted(x.dropna().unique()))),
        )
        .reset_index()
    )

    df_hoja3 = (
        df_con_zfer[['ORDEN', 'LOTE', 'ZFER', 'ZFOR', 'ESTADO_PREEMPAQUE',
                      'fecha_orden', 'm2_y3']]
        .merge(resumen_bom.rename(columns={'MATERIAL': 'ZFER'}),
               on='ZFER', how='left')
        .sort_values(['ORDEN'])
    )

    # Exportar
    OUTPUT.parent.mkdir(exist_ok=True)
    with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
        df_hoja1.to_excel(writer, sheet_name='ordenes_zfer', index=False)
        df_hoja2.to_excel(writer, sheet_name='bom_por_zfer', index=False)
        df_hoja3.to_excel(writer, sheet_name='receta_orden',  index=False)

    # Resumen
    print(f'\n{"=" * 60}')
    print(f'Lotes Y3 desde IM02              : {len(lotes_y3):,}')
    print(f'Órdenes con Y3                   : {len(ordenes_y3):,}')
    print(f'Órdenes Y3 con ZFER              : {df_preempq_y3["ORDEN"].nunique():,}')
    print(f'ZFER únicos                      : {len(zfer_y3):,}')
    print(f'ZFER con ZFOR                    : {zfer_con_zfor:,}')
    print(f'Componentes BOM totales          : {len(df_bom):,}')
    print(f'Filas ordenes_zfer               : {len(df_hoja1):,}')
    sin_zfer = df_hoja1['ZFER'].isna().sum()
    print(f'Órdenes sin ZFER en preempaque   : {sin_zfer:,}')
    print(f'Filas receta completa            : {len(df_hoja3):,}')
    print(f'{"=" * 60}')
    print(f'Exportado: {OUTPUT}')
