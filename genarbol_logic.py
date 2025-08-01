import os
import pandas as pd
import re
from datetime import datetime

# === ENTRADAS ===
ARCHIVO_SCHEDULE = os.path.join("data", "vw_Schedule.csv")
ARCHIVO_MATRIZ = os.path.join("data", "vw_Schedule_Matriz.csv")

# === FUNCIONES AUXILIARES ===
def extraer_nombre_tipo(predecesores):
    if pd.isna(predecesores):
        return []
    pares = []
    partes = [p.strip() for p in str(predecesores).split('|')]
    for parte in partes:
        if ',' in parte:
            nombre, tipo = map(str.strip, parte.split(',', 1))
            pares.append((nombre, tipo))
    return pares

def evaluar_futuro(fecha):
    hoy = pd.to_datetime(datetime.now().date())
    if pd.isna(fecha):
        return '⚠️ Sin fecha'
    elif fecha > hoy:
        dias = (fecha - hoy).days
        return f'🔵 Sí, futura (faltan {dias} días)'
    else:
        return '✅ No'

def generar_arbol_dependencias(layout_base):
    df = pd.read_csv(ARCHIVO_SCHEDULE, sep='|', dtype=str)
    df.columns = df.columns.str.strip()
    df['ScheduleCD'] = pd.to_numeric(df['ScheduleCD'], errors='coerce')
    df['ScheduleCDPred'] = pd.to_numeric(df['ScheduleCDPred'], errors='coerce')
    df_matriz = pd.read_csv(ARCHIVO_MATRIZ, sep='|', dtype=str)
    df_matriz.columns = df_matriz.columns.str.strip()
    df_matriz['ScheduleCD'] = pd.to_numeric(df_matriz['ScheduleCD'], errors='coerce')

    df_base = df[df['NombreLayout'] == layout_base][['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
    df_base['TipoRelacion'] = 'BASE'
    df_base['Nivel'] = 1

    pred_df = df_base.copy()
    for i in range(1, 10):
        step_df = df.merge(pred_df, left_on='ScheduleCD', right_on='ScheduleCDPred')
        step_df = step_df[['ScheduleCD_x', 'ScheduleCDPred_x', 'Predecesores_x']]
        step_df.columns = ['ScheduleCD', 'ScheduleCDPred', 'Predecesores']
        step_df['TipoRelacion'] = f'PADRE+{i}'
        step_df['Nivel'] = i + 1
        if step_df.empty:
            break
        pred_df = pd.concat([pred_df, step_df], ignore_index=True)
    pred_df = pred_df.sort_values(by='Nivel').drop_duplicates(subset='ScheduleCD', keep='first')

    suc_df = df_base.copy()
    for i in range(1, 10):
        step_df = df.merge(suc_df, left_on='ScheduleCDPred', right_on='ScheduleCD')
        step_df = step_df[['ScheduleCD_x', 'ScheduleCDPred_x', 'Predecesores_x']]
        step_df.columns = ['ScheduleCD', 'ScheduleCDPred', 'Predecesores']
        step_df['TipoRelacion'] = f'SUCESOR+{i}'
        step_df['Nivel'] = i + 1
        if step_df.empty:
            break
        suc_df = pd.concat([suc_df, step_df], ignore_index=True)
    suc_df = suc_df.sort_values(by='Nivel').drop_duplicates(subset='ScheduleCD', keep='first')

    arbol_union = pd.concat([pred_df, suc_df], ignore_index=True).drop_duplicates(subset='ScheduleCD')

    nombres_layout_usados = df[df['ScheduleCD'].isin(arbol_union['ScheduleCD'])][['ScheduleCD', 'NombreLayout']].drop_duplicates()
    nombres_layout_unicos = nombres_layout_usados['NombreLayout'].dropna().unique()

    def contiene_nombre(preds):
        if pd.isna(preds):
            return False
        return any(re.search(rf'\b{re.escape(nombre)}\b', preds) for nombre in nombres_layout_unicos)

    df_multiples = df[df['ScheduleCDPred'] == 0].copy()
    df_multiples = df_multiples[df_multiples['Predecesores'].apply(contiene_nombre)]
    df_multiples['TipoRelacion'] = 'MULTIPLE'
    df_multiples['Nivel'] = 1

    arbol_union = pd.concat([arbol_union, df_multiples], ignore_index=True).drop_duplicates(subset='ScheduleCD')

    df_predsusemulti = arbol_union.copy()
    nombres_tipos_extraidos = []
    for fila in df_predsusemulti['Predecesores'].dropna():
        nombres_tipos_extraidos.extend(extraer_nombre_tipo(fila))
    df_nombres_tipos = pd.DataFrame(nombres_tipos_extraidos, columns=['NombreLayout_M', 'TipoSchd_M']).drop_duplicates()

    df_codigos_multiples = df.merge(
        df_nombres_tipos,
        left_on=['NombreLayout', 'TipoSchd'],
        right_on=['NombreLayout_M', 'TipoSchd_M'],
        how='inner'
    )[['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
    df_codigos_multiples['TipoRelacion'] = 'MULTIPLE_NOMBRE'
    df_codigos_multiples['Nivel'] = 1

    arbol_union = pd.concat([arbol_union, df_codigos_multiples], ignore_index=True).drop_duplicates(subset='ScheduleCD')
    df_sucesores = arbol_union.copy()
    df_pred_final = arbol_union.copy()
    visitados = set(arbol_union['ScheduleCD'])
    visitados_atras = set(arbol_union['ScheduleCD'])

    nivel = 1
    while True:
        nivel += 1
        if nivel > 100:
            break
        nuevos = df[df['ScheduleCDPred'].isin(df_sucesores['ScheduleCD']) & ~df['ScheduleCD'].isin(visitados)]
        if nuevos.empty:
            break
        nuevos = nuevos[['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
        nuevos['TipoRelacion'] = f'SUCESOR+{nivel-1}'
        nuevos['Nivel'] = nivel
        visitados.update(nuevos['ScheduleCD'])
        df_sucesores = pd.concat([df_sucesores, nuevos], ignore_index=True)

    nivel = 1
    while True:
        nivel += 1
        if nivel > 100:
            break
        nuevos = df[df['ScheduleCD'].isin(df_pred_final['ScheduleCDPred']) & ~df['ScheduleCD'].isin(visitados_atras)]
        if nuevos.empty:
            break
        nuevos = nuevos[['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
        nuevos['TipoRelacion'] = f'PADRE+{nivel-1}'
        nuevos['Nivel'] = nivel
        visitados_atras.update(nuevos['ScheduleCD'])
        df_pred_final = pd.concat([df_pred_final, nuevos], ignore_index=True)

    arbol_final = pd.concat([df_pred_final, df_sucesores], ignore_index=True).drop_duplicates(subset='ScheduleCD', keep='first')

    df_info = df[['ScheduleCD', 'NombreLayout', 'TipoSchd', 'UnidadFrecCD', 'FecIni', 'HorIni', 'ValorFrecuenciaAdic']].drop_duplicates(subset='ScheduleCD')
    resultado = arbol_final.merge(df_info, on='ScheduleCD', how='left', suffixes=('', '_csv'))
    for col in ['NombreLayout', 'TipoSchd', 'UnidadFrecCD', 'FecIni', 'HorIni', 'ValorFrecuenciaAdic']:
        col_csv = f'{col}_csv'
        if col_csv in resultado.columns:
            resultado[col] = resultado.apply(
                lambda row: row[col_csv] if pd.isna(row[col]) or str(row[col]).strip() in ['', 'NaT', 'nan', 'NaN'] else row[col],
                axis=1
            )

    resultado['FecIni'] = pd.to_datetime(resultado['FecIni'], format='%d/%m/%Y', errors='coerce').dt.strftime('%d/%m/%Y')
    resultado['HorIni'] = pd.to_datetime(resultado['HorIni'], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M')
    resultado['FecIni_dt'] = pd.to_datetime(resultado['FecIni'], format='%d/%m/%Y', errors='coerce')
    resultado['EsFuturo'] = resultado['FecIni_dt'].apply(evaluar_futuro)
    resultado['UnidadFrecDesc'] = resultado['UnidadFrecCD'].map({
        '100': 'Minuto',
        '101': 'Diario',
        '105': 'Anual',
        '106': 'A DEMANDA',
        '102': 'Mensual',
        '104': 'Semanal',
        '103': 'Hora'
    })

    df_matriz = df_matriz[['ScheduleCD', 'SchdMatrixCD', 'FecIniEjec_TS', 'FecFinEjec_TS', 'DesEstado', 'numEjec']].drop_duplicates(subset='ScheduleCD')
    resultado = resultado.merge(df_matriz, on='ScheduleCD', how='left')
    resultado['Orden'] = resultado['ScheduleCD'].rank(method='dense').astype(int)
    resultado = resultado.sort_values(by='Orden')

    columnas_finales = [
        'Orden', 'ScheduleCD', 'NombreLayout', 'TipoSchd',
        'UnidadFrecDesc', 'FecIni', 'HorIni', 'EsFuturo', 'Predecesores',
        'ValorFrecuenciaAdic', 'SchdMatrixCD', 'FecIniEjec_TS', 'FecFinEjec_TS', 'DesEstado', 'numEjec'
    ]
    return resultado[columnas_finales]
