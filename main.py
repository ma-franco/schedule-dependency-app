import sys
import pandas as pd
import re
from datetime import datetime

# === PARÁMETRO DE ENTRADA: Layout base ===
if len(sys.argv) != 2:
    print("\n⚠️  Debes proporcionar el nombre del Layout base como argumento.")
    print("Ejemplo: python genarbol.py CDRAURALV")
    sys.exit(1)

layout_base = sys.argv[1].strip()

# === 1. LECTURA DE DATOS ===
file_path = '/datos/FG_TEST/InputFiles/vw_Schedule.csv'
df = pd.read_csv(file_path, sep='|', dtype=str)
df.columns = df.columns.str.strip()
df['ScheduleCD'] = pd.to_numeric(df['ScheduleCD'], errors='coerce')
df['ScheduleCDPred'] = pd.to_numeric(df['ScheduleCDPred'], errors='coerce')

# === 1.1. LECTURA DE DATOS ADICIONALES DESDE vw_Schedule_Matriz.csv ===
file_path_matriz = '/datos/FG_TEST/InputFiles/vw_Schedule_Matriz.csv'
df_matriz = pd.read_csv(file_path_matriz, sep='|', dtype=str)
df_matriz.columns = df_matriz.columns.str.strip()
df_matriz['ScheduleCD'] = pd.to_numeric(df_matriz['ScheduleCD'], errors='coerce')


# === 2. LAYOUT BASE ===
df_base = df[df['NombreLayout'] == layout_base][['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
df_base['TipoRelacion'] = 'BASE'
df_base['Nivel'] = 1

#print("\n=== BLOQUE 2: LAYOUT BASE ===")
#print(df_base)

# === 3. PREDECESORES (sin duplicados, estilo Teradata) ===
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

# ✅ Aplicar deduplicación manteniendo el nivel más bajo (como QUALIFY ROW_NUMBER())
pred_df = pred_df.sort_values(by='Nivel') \
                 .drop_duplicates(subset='ScheduleCD', keep='first') \
                 .reset_index(drop=True)

#print("\n=== BLOQUE 3: PREDECESORES (sin duplicados) ===")
#print(pred_df)

# === 4. SUCESORES (sin duplicados, estilo Teradata) ===
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

# ✅ Aplicar deduplicación al estilo QUALIFY ROW_NUMBER()
suc_df = suc_df.sort_values(by='Nivel') \
               .drop_duplicates(subset='ScheduleCD', keep='first') \
               .reset_index(drop=True)

#print("\n=== BLOQUE 4: SUCESORES (sin duplicados) ===")
#print(suc_df)


# === 5. UNIÓN PREDECESORES + SUCESORES ===
arbol_union_simple = pd.concat([pred_df, suc_df], ignore_index=True)
arbol_union_simple = arbol_union_simple.drop_duplicates(subset='ScheduleCD')

#print("\n=== BLOQUE 5: UNIÓN PREDECESORES + SUCESORES ===")
#print(arbol_union_simple)

# === 6. IDENTIFICACIÓN DE MÚLTIPLES ===
nombres_layout_usados = df[df['ScheduleCD'].isin(arbol_union_simple['ScheduleCD'])][['ScheduleCD', 'NombreLayout']].drop_duplicates()
nombres_layout_unicos = nombres_layout_usados['NombreLayout'].dropna().unique()

def contiene_nombre(preds, nombres):
    if pd.isna(preds):
        return False
    return any(re.search(rf'\b{re.escape(nombre)}\b', preds) for nombre in nombres)

df_multiples = df[df['ScheduleCDPred'] == 0].copy()
df_multiples = df_multiples[df_multiples['Predecesores'].apply(lambda x: contiene_nombre(x, nombres_layout_unicos))]
df_multiples['TipoRelacion'] = 'MULTIPLE'
df_multiples['Nivel'] = 1

#print("\n=== BLOQUE 6: MÚLTIPLES DETECTADOS ===")
#print(df_multiples)

arbol_union_multiples = pd.concat([arbol_union_simple, df_multiples], ignore_index=True)
arbol_union_multiples = arbol_union_multiples.drop_duplicates(subset='ScheduleCD')

# === BLOQUE 6.1: EXTRAER MÚLTIPLES POR NOMBRE Y TIPO ===
#print("\n=== BLOQUE 6.1: EXTRACCIÓN DE MÚLTIPLES POR NOMBRE Y TIPO ===")

# Crear un DataFrame con los valores actuales del árbol hasta el bloque 6
df_predsusemulti = arbol_union_multiples.copy()

# Lista para acumular pares (NombreLayout, TipoSchd) extraídos
nombres_tipos_extraidos = []

# Función auxiliar para dividir el campo Predecesores
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

# Aplicar la extracción a todas las filas
for fila in df_predsusemulti['Predecesores'].dropna():
    nombres_tipos_extraidos.extend(extraer_nombre_tipo(fila))

# Convertir a DataFrame y eliminar duplicados
df_nombres_tipos = pd.DataFrame(nombres_tipos_extraidos, columns=['NombreLayout_M', 'TipoSchd_M']).drop_duplicates()

#print("🎯 Pares NombreLayout y TipoSchd extraídos desde Predecesores:")
#print(df_nombres_tipos)

# === Buscar ScheduleCDs asociados a esos pares en el CSV original ===
df_codigos_multiples = df.merge(
    df_nombres_tipos,
    left_on=['NombreLayout', 'TipoSchd'],
    right_on=['NombreLayout_M', 'TipoSchd_M'],
    how='inner'
)[['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()

df_codigos_multiples['TipoRelacion'] = 'MULTIPLE_NOMBRE'
df_codigos_multiples['Nivel'] = 1

#print("\n🧩 Layouts encontrados por nombre y tipo (múltiples):")
#print(df_codigos_multiples)

# === Agregar estos nuevos layouts al árbol base (para expansión posterior) ===
arbol_union_completo = pd.concat([arbol_union_multiples, df_codigos_multiples], ignore_index=True)
arbol_union_completo = arbol_union_completo.drop_duplicates(subset='ScheduleCD')

# Usar arbol_union_completo (incluye múltiples detectados por nombre y tipo)
df_sucesores = arbol_union_completo.copy()
df_pred_final = arbol_union_completo.copy()
visitados = set(arbol_union_completo['ScheduleCD'])
visitados_atras = set(arbol_union_completo['ScheduleCD'])


# === 7. EXPANSIÓN RECURSIVA HACIA ADELANTE ===
#print("\n=== BLOQUE 7: EXPANSIÓN RECURSIVA HACIA ADELANTE ===")
nivel = 1
visitados = set(arbol_union_completo['ScheduleCD'])
df_sucesores = arbol_union_completo.copy()

while True:
    nivel += 1
    if nivel > 100:
        print("⚠️ Nivel máximo alcanzado en expansión hacia adelante.")
        break

    nuevos = df[df['ScheduleCDPred'].isin(df_sucesores['ScheduleCD']) & ~df['ScheduleCD'].isin(visitados)]
    if nuevos.empty:
        break

    nuevos = nuevos[['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
    nuevos['TipoRelacion'] = f'SUCESOR+{nivel-1}'
    nuevos['Nivel'] = nivel
    visitados.update(nuevos['ScheduleCD'])
    df_sucesores = pd.concat([df_sucesores, nuevos], ignore_index=True)

#print(df_sucesores)

# === 8. EXPANSIÓN RECURSIVA HACIA ATRÁS ===
#print("\n=== BLOQUE 8: EXPANSIÓN RECURSIVA HACIA ATRÁS ===")
nivel = 1
visitados_atras = set(arbol_union_completo['ScheduleCD'])
df_pred_final = arbol_union_completo.copy()

while True:
    nivel += 1
    if nivel > 100:
        print("⚠️ Nivel máximo alcanzado en expansión hacia atrás.")
        break

    nuevos = df[df['ScheduleCD'].isin(df_pred_final['ScheduleCDPred']) & ~df['ScheduleCD'].isin(visitados_atras)]
    if nuevos.empty:
        break

    nuevos = nuevos[['ScheduleCD', 'ScheduleCDPred', 'Predecesores']].copy()
    nuevos['TipoRelacion'] = f'PADRE+{nivel-1}'
    nuevos['Nivel'] = nivel
    visitados_atras.update(nuevos['ScheduleCD'])
    df_pred_final = pd.concat([df_pred_final, nuevos], ignore_index=True)

#print(df_pred_final)

# === 9. UNIÓN FINAL Y DEDUPLICACIÓN ===
#print("\n=== BLOQUE 9: UNIÓN FINAL Y DEDUPLICACIÓN ===")
arbol_final = pd.concat([df_pred_final, df_sucesores], ignore_index=True)
arbol_final = arbol_final.drop_duplicates(subset='ScheduleCD', keep='first')

#print(arbol_final)

# === 10. ENRIQUECER CON DATOS DEL CSV (como un JOIN SQL) ===
#print("\n=== BLOQUE 10: ENRIQUECER CON DATOS DEL CSV ===")
#df_info = df[['ScheduleCD', 'NombreLayout', 'TipoSchd', 'UnidadFrecCD', 'FecIni', 'HorIni']].drop_duplicates(subset='ScheduleCD')
df_info = df[['ScheduleCD', 'NombreLayout', 'TipoSchd', 'UnidadFrecCD', 'FecIni', 'HorIni', 'ValorFrecuenciaAdic']].drop_duplicates(subset='ScheduleCD')
resultado = arbol_final.merge(df_info, on='ScheduleCD', how='left', suffixes=('', '_csv'))

# Reemplazar valores vacíos con *_csv si existen
for col in ['NombreLayout', 'TipoSchd', 'UnidadFrecCD', 'FecIni', 'HorIni', 'ValorFrecuenciaAdic']:
    col_csv = f'{col}_csv'
    if col_csv in resultado.columns:
        resultado[col] = resultado.apply(
            lambda row: row[col_csv]
            if pd.isna(row[col]) or str(row[col]).strip() in ['', 'NaT', 'nan', 'NaN']
            else row[col],
            axis=1
        )

# Formatear FecIni y HorIni
resultado['FecIni'] = pd.to_datetime(resultado['FecIni'], format='%d/%m/%Y', errors='coerce').dt.strftime('%d/%m/%Y')
resultado['HorIni'] = pd.to_datetime(resultado['HorIni'], format='%H:%M:%S', errors='coerce').dt.strftime('%H:%M')

# Evaluar si FecIni es futura
resultado['FecIni_dt'] = pd.to_datetime(resultado['FecIni'], format='%d/%m/%Y', errors='coerce')
hoy = pd.to_datetime(datetime.now().date())

def evaluar_futuro(fecha):
    if pd.isna(fecha):
        return '⚠️ Sin fecha'
    elif fecha > hoy:
        dias = (fecha - hoy).days
        return f'🔵 Sí, futura (faltan {dias} días)'
    else:
        return '✅ No ' #, actual/pasada'

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

# === 10.1 ENRIQUECER CON DATOS DE MATRIZ ===
df_matriz = df_matriz[['ScheduleCD', 'SchdMatrixCD', 'FecIniEjec_TS', 'FecFinEjec_TS', 'DesEstado', 'numEjec']].drop_duplicates(subset='ScheduleCD')
resultado = resultado.merge(df_matriz, on='ScheduleCD', how='left')

# Validación de faltantes
faltantes = resultado[resultado['NombreLayout'].isna()]
print("\n❗ ScheduleCD sin NombreLayout encontrados:")
print(faltantes[['ScheduleCD', 'ScheduleCDPred', 'TipoRelacion']])

# ORDEN Y RESULTADO FINAL
resultado['Orden'] = resultado['ScheduleCD'].rank(method='dense').astype(int)
resultado = resultado.sort_values(by='Orden')

columnas_finales = [
    'Orden', 'ScheduleCD', 'NombreLayout', 'TipoSchd',
    #'UnidadFrecCD', 
    'UnidadFrecDesc', 'FecIni', 'HorIni',
    'EsFuturo', 'Predecesores',
    #, 'TipoRelacion' , 'ScheduleCDPred'
    'ValorFrecuenciaAdic', 'SchdMatrixCD', 'FecIniEjec_TS', 'FecFinEjec_TS', 'DesEstado', 'numEjec'
]
resultado = resultado[columnas_finales]

print("\n=== RESULTADO FINAL ===")
print(resultado)

# === EXPORTAR RESULTADO A CSV CON FECHA Y HORA EN RUTA ESPECÍFICA ===
fecha_actual = datetime.now().strftime('%Y%m%d_%H%M%S')
nombre_archivo = f"/datos/FG_TEST/OutputFiles/arbol_dependencias_matriz_{layout_base}_{fecha_actual}.csv"

# Archivo técnico con separador múltiple '}' seguido de '<'
with open(nombre_archivo, 'w', encoding='utf-8-sig') as f:
    # Escribir encabezado
    f.write('}<'.join(resultado.columns) + '\n')
    # Escribir cada fila
    for _, row in resultado.iterrows():
        valores = [str(val) if pd.notna(val) else '' for val in row]
        f.write('}<'.join(valores) + '\n')

print(f"\n✅ Archivo generado correctamente con delimitador '}}<': {nombre_archivo}")

#  Archivo para Excel con separador estándar '|'
nombre_archivo_excel = f"/datos/FG_TEST/OutputFiles/arbol_dependencias_matriz_{layout_base}_{fecha_actual}_excel.csv"
resultado.to_csv(nombre_archivo_excel, sep=';', index=False, encoding='utf-8-sig')
print(f"✅ Archivo para Excel correctamente con delimitador ';': {nombre_archivo_excel}")
