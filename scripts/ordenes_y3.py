"""
Organiza las órdenes de producción que consumieron material Y3,
mostrando todos los materiales consumidos por orden (receta).

Genera outputs/ordenes_y3.xlsx con 3 hojas:
  - resumen_ordenes  : una fila por orden, con todos sus materiales y cantidades
  - detalle          : una fila por orden+material (formato largo, útil para análisis)
  - recetas          : agrupación por combinación de materiales (patrones de receta)
"""
import pandas as pd
from pathlib import Path

PARQUET         = Path('data/mb51.parquet')
PARQUET_PREEMPQ = Path('data/preempaque.parquet')
OUTPUT          = Path('outputs/ordenes_y3.xlsx')


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
        return pd.DataFrame(columns=['ORDEN', 'ESTADO_PREEMPAQUE'])
    df = pd.read_parquet(PARQUET_PREEMPQ)
    df = df[['ORDEN']].drop_duplicates()
    df['ESTADO_PREEMPAQUE'] = 'PREEMPAQUE'
    return df


if __name__ == '__main__':
    print('Cargando parquets...')
    df = cargar_parquet()
    df_preempq = cargar_preempaque()

    # ── 1. Órdenes que consumieron Y3 (directo o vía MODMED con lote Y3) ────────
    mask_consumo = df['CLASE_MOV'].isin(['261', '201']) & df['ALMACEN'].isin(['IM01', 'PP04'])

    mask_directo   = mask_consumo & df['TEXTO_MATE'].str.contains('y3', case=False, na=False)
    lotes_y3       = set(df[mask_directo]['LOTE'].unique())
    mask_indirecto = (
        mask_consumo &
        df['TEXTO_MATE'].str.contains('modmed', case=False, na=False) &
        df['LOTE'].isin(lotes_y3)
    )

    ordenes_y3 = set(df[mask_directo | mask_indirecto]['ORDEN'].unique())
    print(f'Ordenes con Y3 directo          : {df[mask_directo]["ORDEN"].nunique():,}')
    print(f'Ordenes con MODMED lote Y3      : {df[mask_indirecto]["ORDEN"].nunique():,}')
    print(f'Total ordenes con Y3            : {len(ordenes_y3):,}')

    # Todos los movimientos clase 261/201 de esas órdenes (receta completa)
    df_ord = df[
        df['ORDEN'].isin(ordenes_y3) &
        df['CLASE_MOV'].isin(['261', '201'])
    ].copy()

    # ── 2. Detalle: una fila por orden + material ──────────────────────────────
    detalle = (
        df_ord.groupby(['ORDEN', 'MATERIAL', 'TEXTO_MATE', 'UNIDAD'])
        .agg(
            m2_consumidos =('CANTIDAD_N', lambda x: x.abs().sum()),
            importe_COP   =('IMPORTE_N',  lambda x: x.abs().sum()),
            lotes_usados  =('LOTE',       'nunique'),
            movimientos   =('ID',         'count'),
            primera_fecha =('FECHA',      'min'),
            ultima_fecha  =('FECHA',      'max'),
        )
        .reset_index()
        .sort_values(['ORDEN', 'MATERIAL'])
    )
    # Materiales MODMED que en estas órdenes usaron lotes Y3
    modmed_y3_materiales = set(
        df[mask_indirecto]['MATERIAL'].unique()
    )
    detalle['es_y3'] = (
        detalle['TEXTO_MATE'].str.contains('y3', case=False, na=False) |
        detalle['MATERIAL'].isin(modmed_y3_materiales)
    )
    print(f'Filas en detalle (orden x material): {len(detalle):,}')

    # ── 3. Resumen: una fila por orden ─────────────────────────────────────────
    def agg_orden(g):
        mats_y3    = g[g['es_y3']]['TEXTO_MATE'].tolist()
        mats_otros = g[~g['es_y3']]['TEXTO_MATE'].tolist()
        return pd.Series({
            'fecha_orden'       : g['primera_fecha'].min(),
            'n_materiales'      : len(g),
            'materiales_y3'     : ' | '.join(sorted(mats_y3)),
            'otros_materiales'  : ' | '.join(sorted(mats_otros)),
            'importe_total_COP' : g['importe_COP'].sum(),
            'm2_y3'             : g.loc[g['es_y3'], 'm2_consumidos'].sum(),
            'm2_total'          : g['m2_consumidos'].sum(),
        })

    print('Construyendo resumen por orden...')
    resumen = detalle.groupby('ORDEN').apply(agg_orden).reset_index()
    
    # Cruzar con preempaque
    resumen = resumen.merge(df_preempq, on='ORDEN', how='left')
    resumen['ESTADO_PREEMPAQUE'] = resumen['ESTADO_PREEMPAQUE'].fillna('PENDIENTE')
    
    resumen = resumen.sort_values('fecha_orden', ascending=False)
    print(f'Ordenes en resumen: {len(resumen):,}')

    # ── 4. Recetas: patrones de combinación de materiales ─────────────────────
    # Una "receta" = conjunto de materiales usados en una orden (normalizado)
    receta_por_orden = (
        detalle.groupby('ORDEN')['TEXTO_MATE']
        .apply(lambda x: ' + '.join(sorted(x.unique())))
        .reset_index()
        .rename(columns={'TEXTO_MATE': 'receta'})
    )
    recetas = (
        receta_por_orden.groupby('receta')
        .agg(veces_usada=('ORDEN', 'count'))
        .reset_index()
        .sort_values('veces_usada', ascending=False)
    )
    print(f'\nTop 10 recetas mas frecuentes:')
    print(recetas.head(10).to_string(index=False))

    # ── 5. Exportar ────────────────────────────────────────────────────────────
    OUTPUT.parent.mkdir(exist_ok=True)
    with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
        resumen.to_excel(writer, sheet_name='resumen_ordenes', index=False)
        detalle.to_excel(writer, sheet_name='detalle',         index=False)
        recetas.to_excel(writer, sheet_name='recetas',         index=False)
    print(f'\nExportado: {OUTPUT}')
