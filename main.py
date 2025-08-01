import streamlit as st
import pandas as pd
import numpy as np
import re
from datetime import datetime
import io
import os

# === RUTAS DE ARCHIVOS CSV ===
ARCHIVO_SCHEDULE = os.path.join("data", "vw_Schedule.csv")
ARCHIVO_MATRIZ = os.path.join("data", "vw_Schedule_Matriz.csv")

# === FUNCIÓN PRINCIPAL: Genera el árbol completo a partir de un layout base ===
@st.cache_data
def generar_arbol(layout_base):
    df = pd.read_csv(ARCHIVO_SCHEDULE, sep='|', dtype=str)
    df.columns = df.columns.str.strip()
    df['ScheduleCD'] = pd.to_numeric(df['ScheduleCD'], errors='coerce')
    df['ScheduleCDPred'] = pd.to_numeric(df['ScheduleCDPred'], errors='coerce')

    df_matriz = pd.read_csv(ARCHIVO_MATRIZ, sep='|', dtype=str)
    df_matriz.columns = df_matriz.columns.str.strip()
    df_matriz['ScheduleCD'] = pd.to_numeric(df_matriz['ScheduleCD'], errors='coerce')

    # Base
    df_base = df[df['NombreLayout'] == layout_base][['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
    if df_base.empty:
        return pd.DataFrame()  # No encontrado

    df_base['TipoRelacion'] = 'BASE'
    df_base['Nivel'] = 1

    # Predecesores
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

    # Sucesores
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

    # Unión inicial
    arbol_union_simple = pd.concat([pred_df, suc_df], ignore_index=True)
    arbol_union_simple = arbol_union_simple.drop_duplicates(subset='ScheduleCD')

    # Múltiples
    nombres_layout_usados = df[df['ScheduleCD'].isin(arbol_union_simple['ScheduleCD'])][['ScheduleCD', 'NombreLayout']]
    nombres_unicos = nombres_layout_usados['NombreLayout'].dropna().unique()

    def contiene_nombre(preds, nombres):
        if pd.isna(preds):
            return False
        return any(re.search(rf'\b{re.escape(nombre)}\b', preds) for nombre in nombres)

    df_multiples = df[df['ScheduleCDPred'] == 0].copy()
    df_multiples = df_multiples[df_multiples['Predecesores'].apply(lambda x: contiene_nombre(x, nombres_unicos))]
    df_multiples['TipoRelacion'] = 'MULTIPLE'
    df_multiples['Nivel'] = 1

    arbol_union = pd.concat([arbol_union_simple, df_multiples], ignore_index=True)
    arbol_union = arbol_union.drop_duplicates(subset='ScheduleCD')

    # Enriquecer
    df_info = df[['ScheduleCD', 'NombreLayout', 'TipoSchd', 'UnidadFrecCD', 'FecIni', 'HorIni', 'ValorFrecuenciaAdic']].drop_duplicates(subset='ScheduleCD')
    resultado = arbol_union.merge(df_info, on='ScheduleCD', how='left', suffixes=('', '_csv'))

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
    hoy = pd.to_datetime(datetime.now().date())

    def evaluar_futuro(fecha):
        if pd.isna(fecha):
            return '⚠️ Sin fecha'
        elif fecha > hoy:
            dias = (fecha - hoy).days
            return f'🔵 Sí, futura (faltan {dias} días)'
        else:
            return '✅ No'

    resultado['EsFuturo'] = resultado['FecIni_dt'].apply(evaluar_futuro)
    resultado['UnidadFrecDesc'] = resultado['UnidadFrecCD'].map({
        '100': 'Minuto', '101': 'Diario', '105': 'Anual',
        '106': 'A DEMANDA', '102': 'Mensual', '104': 'Semanal', '103': 'Hora'
    })

    df_matriz = df_matriz[['ScheduleCD', 'SchdMatrixCD', 'FecIniEjec_TS', 'FecFinEjec_TS', 'DesEstado', 'numEjec']].drop_duplicates(subset='ScheduleCD')
    resultado = resultado.merge(df_matriz, on='ScheduleCD', how='left')

    resultado['Orden'] = resultado['ScheduleCD'].rank(method='dense').astype(int)
    resultado = resultado.sort_values(by='Orden')

    columnas_finales = [
        'Orden', 'ScheduleCD', 'NombreLayout', 'TipoSchd',
        'UnidadFrecDesc', 'FecIni', 'HorIni', 'EsFuturo',
        'Predecesores', 'ValorFrecuenciaAdic', 'SchdMatrixCD',
        'FecIniEjec_TS', 'FecFinEjec_TS', 'DesEstado', 'numEjec'
    ]
    return resultado[columnas_finales]

# === INTERFAZ DE USUARIO STREAMLIT ===
st.set_page_config(layout="wide", page_title="Árbol de Dependencias")
st.title("🌳 Generador de Árbol de Dependencias")

layout_input = st.text_input("🔍 Ingrese el Nombre del Layout base:", "")
btn_generar = st.button("🚀 Generar Árbol")

if btn_generar and layout_input.strip():
    with st.spinner("Generando árbol..."):
        resultado_df = generar_arbol(layout_input.strip())
        if resultado_df.empty:
            st.error("❌ No se encontró el Layout proporcionado en el archivo CSV.")
        else:
            st.success(f"✅ Árbol generado con {len(resultado_df)} layouts.")
            st.dataframe(resultado_df, use_container_width=True)

            # Descarga
            separador = st.selectbox("Selecciona el separador para exportar:", ['|', ';', '},<'])
            buffer = io.StringIO()

            if separador == '},<':
                buffer.write('}<'.join(resultado_df.columns) + '\n')
                for _, row in resultado_df.iterrows():
                    valores = [str(val) if pd.notna(val) else '' for val in row]
                    buffer.write('}<'.join(valores) + '\n')
            else:
                resultado_df.to_csv(buffer, sep=separador, index=False)

            st.download_button(
                label="📥 Descargar resultado",
                data=buffer.getvalue(),
                file_name=f"arbol_{layout_input.strip()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
