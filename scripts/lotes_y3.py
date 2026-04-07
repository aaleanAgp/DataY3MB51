"""
Extrae recetas completas (BOM real) de las órdenes que consumieron material Y3.

Genera:
  - DataFrame `recetas_completas`: Todos los materiales consumidos en órdenes con Y3.
  - outputs/recetas_y3.xlsx : Exportación lista para analizar ratios y mermas.
"""
import pandas as pd
from pathlib import Path

PARQUET = Path('data/mb51.parquet')
OUTPUT  = Path('outputs/recetas_y3.xlsx')

def parse_sap_num(serie: pd.Series) -> pd.Series:
    """Convierte strings SAP con signo al final ('123.45-') a float."""
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

def pescar_recetas_y3(df: pd.DataFrame) -> pd.DataFrame:
    """Usa Y3 como anzuelo para extraer todas las órdenes completas."""
    # 1. Encontrar las órdenes (únicas) que tocaron material Y3
    mascara_y3 = df['TEXTO_MATE'].str.contains('y3', case=False, na=False)
    ordenes_con_y3 = df[mascara_y3]['ORDEN'].unique()
    
    # 2. Extraer TODOS los movimientos de esas órdenes (la receta completa)
    # isin() es rapidísimo para filtrar contra listas grandes
    movimientos_completos = df[df['ORDEN'].isin(ordenes_con_y3)].copy()
    
    return movimientos_completos

def consolidar_ordenes(df_recetas: pd.DataFrame) -> pd.DataFrame:
    """Agrupa los movimientos por Orden y Material para ver la receta real."""
    return (
        df_recetas.groupby(['ORDEN', 'MATERIAL', 'TEXTO_MATE', 'LOTE'])
        .agg(
            movimientos_sap =('ID',         'count'),
            primera_fecha   =('FECHA',      'min'),
            cantidad_total  =('CANTIDAD_N', lambda x: x.abs().sum()),
            costo_COP       =('IMPORTE_N',  lambda x: x.abs().sum()),
            # Tomamos la unidad del primer registro para saber si es M2, KG, EA, etc.
            unidad          =('UNIDAD',     'first') 
        )
        .reset_index()
        # Ordenamos para ver primero la Orden, y adentro los materiales de mayor a menor consumo
        .sort_values(['ORDEN', 'costo_COP'], ascending=[True, False])
    )

if __name__ == '__main__':
    print('Cargando parquet con movimientos MB51...')
    df_mb51 = cargar_parquet()

    print('Pescando órdenes que contienen Y3 y reconstruyendo recetas...')
    df_movs_recetas = pescar_recetas_y3(df_mb51)

    print('Consolidando consumos por Orden...')
    df_recetas_consolidadas = consolidar_ordenes(df_movs_recetas)

    # Resumen gerencial en consola
    print(f'\n{"="*55}')
    print(f'Órdenes únicas encontradas con Y3 : {df_movs_recetas["ORDEN"].nunique():,}')
    print(f'Total de componentes en recetas   : {len(df_recetas_consolidadas):,}')
    print(f'{"="*55}')

    # Mostrar un pequeño ejemplo de cómo se ve una sola orden consolidada
    if not df_recetas_consolidadas.empty:
        orden_ejemplo = df_recetas_consolidadas['ORDEN'].iloc[0]
        print(f"\nEjemplo de la Receta Consolidada para la Orden {orden_ejemplo}:")
        print(df_recetas_consolidadas[df_recetas_consolidadas['ORDEN'] == orden_ejemplo][['TEXTO_MATE', 'cantidad_total', 'unidad', 'costo_COP']].to_string(index=False))

    # Exportar a Excel
    OUTPUT.parent.mkdir(exist_ok=True)
    with pd.ExcelWriter(OUTPUT, engine='openpyxl') as writer:
        # Pestaña 1: La receta agrupada (Ideal para análisis)
        df_recetas_consolidadas.to_excel(writer, sheet_name='Recetas_Consolidadas', index=False)
        # Pestaña 2: El crudo de esos movimientos por si hay dudas
        df_movs_recetas.to_excel(writer, sheet_name='Movimientos_Crudos', index=False)
        
    print(f'\n[OK] Excel exportado en: {OUTPUT}')