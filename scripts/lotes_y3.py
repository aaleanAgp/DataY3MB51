"""
Informe completo de lotes Y3: desde cada lote, las órdenes que lo consumieron
y los materiales que acompañaron al Y3 en cada orden (contexto de receta).

Genera outputs/lotes_y3.xlsx con 4 hojas:
  1. lotes          : una fila por lote Y3 — métricas de consumo y órdenes
  2. lotes_ordenes  : una fila por (lote Y3, orden) — qué otros materiales usó esa orden
  3. acompañantes   : materiales que más aparecen junto al Y3 (para recetas)
  4. movimientos    : detalle crudo de todos los movimientos Y3
"""
import pandas as pd
from pathlib import Path

PARQUET         = Path('data/mb51.parquet')
PARQUET_PREEMPQ = Path('data/preempaque.parquet')
OUTPUT          = Path('outputs/lotes_y3.xlsx')


# ── Helpers ────────────────────────────────────────────────────────────────

def parse_sap_num(serie: pd.Series) -> pd.Series:
    """Convierte strings SAP con signo al final ('123.45-') a float."""
    s = serie.str.strip()
    negativo = s.str.endswith('-')
    valor = pd.to_numeric(s.str.rstrip('-').str.replace(',', '', regex=False), errors='coerce')
    return valor.where(~negativo, -valor)


def cargar_parquet() -> pd.DataFrame:
    df = pd.read_parquet(PARQUET)
    # Estandarizar ordenes a 12 caracteres (string)
    if 'ORDEN' in df.columns:
        df['ORDEN'] = df['ORDEN'].astype(str).str.strip().str.zfill(12)
    df['CANTIDAD_N'] = parse_sap_num(df['CANTIDAD'])
    df['IMPORTE_N']  = parse_sap_num(df['IMPORTE'])
    df['FECHA']      = pd.to_datetime(df['REGISTRADO_EL'], format='%Y%m%d', errors='coerce')
    df['AÑO']        = df['FECHA'].dt.year
    return df


def cargar_preempaque() -> pd.DataFrame:
    if not PARQUET_PREEMPQ.exists():
        return pd.DataFrame(columns=['ORDEN', 'ESTADO_PREEMPAQUE'])
    df = pd.read_parquet(PARQUET_PREEMPQ)
    # Nos interesa solo saber si existe la orden
    df = df[['ORDEN']].drop_duplicates()
    df['ESTADO_PREEMPAQUE'] = 'PREEMPAQUE'
    return df


# ── Hojas del informe ──────────────────────────────────────────────────────

def hoja_lotes(df_y3: pd.DataFrame) -> pd.DataFrame:
    """Una fila por lote Y3 con métricas de consumo y alcance de órdenes."""
    return (
        df_y3.groupby(['LOTE', 'MATERIAL', 'TEXTO_MATE', 'tipo_y3'])
        .agg(
            primera_orden =('FECHA',      'min'),
            ultima_orden  =('FECHA',      'max'),
            ordenes       =('ORDEN',      'nunique'),
            movimientos   =('ID',         'count'),
            m2_consumidos =('CANTIDAD_N', lambda x: x.abs().sum()),
            importe_COP   =('IMPORTE_N',  lambda x: x.abs().sum()),
        )
        .reset_index()
        .assign(dias_en_uso=lambda d: (d['ultima_orden'] - d['primera_orden']).dt.days)
        .sort_values('ordenes', ascending=False)
        [['LOTE', 'MATERIAL', 'TEXTO_MATE', 'tipo_y3',
          'primera_orden', 'ultima_orden', 'dias_en_uso',
          'ordenes', 'movimientos', 'm2_consumidos', 'importe_COP']]
    )


def hoja_lotes_ordenes(df_y3: pd.DataFrame, df_full: pd.DataFrame, df_preempq: pd.DataFrame) -> pd.DataFrame:
    """
    Una fila por (lote Y3, orden): cuánto Y3 consumió esa orden.
    """
    # Consumo de Y3 por lote+orden
    y3_por_orden = (
        df_y3.groupby(['LOTE', 'TEXTO_MATE', 'ORDEN', 'ALMACEN'])
        .agg(
            fecha_orden    =('FECHA',      'min'),
            m2_y3          =('CANTIDAD_N', lambda x: x.abs().sum()),
        )
        .reset_index()
    )

    res = (
        y3_por_orden
        .merge(df_preempq, on='ORDEN', how='left')
    )
    res['ESTADO_PREEMPAQUE'] = res['ESTADO_PREEMPAQUE'].fillna('PENDIENTE')

    return (
        res
        .sort_values(['LOTE', 'ORDEN'])
        [['LOTE', 'TEXTO_MATE', 'ORDEN', 'ALMACEN', 'ESTADO_PREEMPAQUE', 'fecha_orden', 'm2_y3']]
    )


def hoja_acompañantes(df_full: pd.DataFrame, ordenes_con_y3: set) -> pd.DataFrame:
    """
    Materiales no-Y3 que más aparecen en órdenes que consumieron Y3.
    Base para identificar familias de recetas.
    """
    return (
        df_full[
            df_full['ORDEN'].isin(ordenes_con_y3) &
            df_full['CLASE_MOV'].isin(['261', '201']) &
            ~df_full['TEXTO_MATE'].str.contains('y3', case=False, na=False)
        ]
        .groupby(['MATERIAL', 'TEXTO_MATE'])
        .agg(
            ordenes_con_y3 =('ORDEN',      'nunique'),
            m2_total       =('CANTIDAD_N', lambda x: x.abs().sum()),
            importe_COP    =('IMPORTE_N',  lambda x: x.abs().sum()),
            movimientos    =('ID',         'count'),
        )
        .reset_index()
        .sort_values('ordenes_con_y3', ascending=False)
    )


# ── Main ───────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    print('Cargando parquets...')
    df = cargar_parquet()
    df_preempq = cargar_preempaque()

    print('Identificando lotes Y3 y movimientos asociados...')

    # ── Paso 1: Identificar lotes Y3 desde IM02 (fuente de verdad) ─────────
    mask_im02_y3 = (
        (df['ALMACEN'] == 'IM02') &
        df['TEXTO_MATE'].str.contains('y3', case=False, na=False)
    )
    lotes_y3 = set(df[mask_im02_y3]['LOTE'].unique())
    print(f'  Lotes Y3 identificados en IM02: {len(lotes_y3):,}')

    # ── Paso 2: Buscar consumo de esos lotes en almacenes de producción ────
    mask_consumo = df['CLASE_MOV'].isin(['261', '201']) & df['ALMACEN'].isin(['IM01', 'PP04'])

    # Camino 1 — movimientos que dicen "Y3" en la descripción
    mask_directo = mask_consumo & df['TEXTO_MATE'].str.contains('y3', case=False, na=False)

    # Camino 2 — cualquier material (ej. MODMED) que use lotes Y3
    mask_indirecto = mask_consumo & df['LOTE'].isin(lotes_y3)

    df_y3 = df[mask_directo | mask_indirecto].copy()
    df_y3['tipo_y3'] = df_y3['TEXTO_MATE'].str.contains('y3', case=False, na=False).map(
        {True: 'DIRECTO', False: 'MODMED_LOTE_Y3'}
    )

    ordenes_con_y3 = set(df_y3['ORDEN'].unique())
    n_dir  = mask_directo.sum()
    n_ind  = mask_indirecto.sum()
    print(f'  Directos (TEXTO_MATE Y3)    : {n_dir:,} movimientos')
    print(f'  Indirectos (lote Y3)        : {n_ind:,} movimientos')
    print(f'  Total lotes Y3              : {len(lotes_y3):,}')

    print(f'  {df_y3["LOTE"].nunique():,} lotes  |  {len(ordenes_con_y3):,} ordenes  |  {len(df_y3):,} movimientos')

    print('Construyendo hoja 1: lotes...')
    df_lotes = hoja_lotes(df_y3)

    print('Construyendo hoja 2: lotes x ordenes...')
    df_lotes_ord = hoja_lotes_ordenes(df_y3, df, df_preempq)

    print('Construyendo hoja 3: materiales acompañantes...')
    df_acomp = hoja_acompañantes(df, ordenes_con_y3)

    # Resumen en consola
    print(f'\n{"="*60}')
    print(f'Lotes Y3 distintos          : {len(df_lotes):,}')
    print(f'Ordenes que usaron Y3       : {len(ordenes_con_y3):,}')
    print(f'  - Lotes con 1 sola orden  : {(df_lotes["ordenes"] == 1).sum():,}')
    print(f'  - Lotes con >10 ordenes   : {(df_lotes["ordenes"] > 10).sum():,}')
    print(f'  - Max ordenes por lote    : {df_lotes["ordenes"].max():,}')
    print(f'Top 10 materiales junto al Y3:')
    print(df_acomp[['TEXTO_MATE', 'ordenes_con_y3']].head(10).to_string(index=False))
    print('='*60)

    # Exportar
    OUTPUT.parent.mkdir(exist_ok=True)
    with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
        df_lotes.to_excel(    writer, sheet_name='lotes',         index=False)
        df_lotes_ord.to_excel(writer, sheet_name='lotes_ordenes', index=False)
        df_acomp.to_excel(    writer, sheet_name='acompañantes',  index=False)
        df_y3.to_excel(       writer, sheet_name='movimientos',   index=False)

    print(f'Exportado: {OUTPUT}')
