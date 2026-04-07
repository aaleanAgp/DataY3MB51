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

PARQUET = Path('data/mb51.parquet')
OUTPUT  = Path('outputs/ordenes_y3.xlsx')


def parse_sap_num(serie: pd.Series) -> pd.Series:
    s = serie.str.strip()
    negativo = s.str.endswith('-')
    valor = pd.to_numeric(s.str.rstrip('-').str.replace(',', '', regex=False), errors='coerce')
    return valor.where(~negativo, -valor)


def cargar_parquet() -> pd.DataFrame:
    df = pd.read_parquet(PARQUET)
    df['CANTIDAD_N'] = parse_sap_num(df['CANTIDAD'])
    df['IMPORTE_N']  = parse_sap_num(df['IMPORTE'])
    df['FECHA']      = pd.to_datetime(df['REGISTRADO_EL'], format='%Y%m%d', errors='coerce')
    return df


if __name__ == '__main__':
    print('Cargando parquet...')
    df = cargar_parquet()

    # ── 1. Órdenes que consumieron Y3 ─────────────────────────────────────────
    ordenes_y3 = set(
        df[df['TEXTO_MATE'].str.contains('y3', case=False, na=False)]['ORDEN'].unique()
    )
    print(f'Ordenes con Y3: {len(ordenes_y3):,}')

    # Todos los movimientos de esas órdenes (todos los materiales)
    df_ord = df[df['ORDEN'].isin(ordenes_y3)].copy()

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
    detalle['es_y3'] = detalle['TEXTO_MATE'].str.contains('y3', case=False, na=False)
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
