import os
import pandas as pd
import numpy as np
import re
from datetime import datetime

# === ENTRADAS ===
ARCHIVO_SCHEDULE = os.path.join("data", "vw_Schedule.csv")
ARCHIVO_MATRIZ = os.path.join("data", "vw_Schedule_Matriz.csv")

# === LECTURA DE ARCHIVOS ===
df_schedule = pd.read_csv(ARCHIVO_SCHEDULE, sep='}<', dtype=str)
df_matriz = pd.read_csv(ARCHIVO_MATRIZ, sep='}<', dtype=str)

# === FUNCIONES AUXILIARES ===
def obtener_multiples(row):
    if pd.isna(row['Predecesores']):
        return []
    candidatos = row['Predecesores'].split('|')
    encontrados = []
    for item in candidatos:
        partes = item.split('~')
        if len(partes) == 2:
            nombre, tipo = partes
            encontrados.append((nombre.strip(), tipo.strip()))
    return encontrados

# === ENRIQUECIMIENTO ===
df_schedule['NombreLayout'] = df_schedule['NombreLayout'].fillna('')
df_schedule['TipoSchd'] = df_schedule['TipoSchd'].fillna('')
df_schedule['Clave_M'] = df_schedule['NombreLayout'] + '__' + df_schedule['TipoSchd']
clave_a_schedulecd = dict(zip(df_schedule['Clave_M'], df_schedule['ScheduleCD']))

# === CONSTRUCCIÓN DE ÁRBOL DE DEPENDENCIAS ===
relaciones = []
visitados = set()

def expandir(schedulecd, nivel):
    if nivel > 100 or schedulecd in visitados:
        return
    visitados.add(schedulecd)
    fila = df_schedule[df_schedule['ScheduleCD'] == schedulecd]
    if fila.empty:
        return
    fila = fila.iloc[0]
    sucesores = df_schedule[df_schedule['ScheduleCDPred'] == schedulecd]['ScheduleCD'].tolist()
    for s in sucesores:
        relaciones.append((schedulecd, 'sucesor', s))
        expandir(s, nivel + 1)
    predecesores = df_schedule[df_schedule['ScheduleCD'] == schedulecd]['ScheduleCDPred'].tolist()
    for p in predecesores:
        if p != '0':
            relaciones.append((schedulecd, 'predecesor', p))
            expandir(p, nivel + 1)
    fila_m = obtener_multiples(fila)
    for nombre, tipo in fila_m:
        clave = nombre + '__' + tipo
        if clave in clave_a_schedulecd:
            s_m = clave_a_schedulecd[clave]
            relaciones.append((schedulecd, 'multiple', s_m))
            expandir(s_m, nivel + 1)

# === INICIAR DESDE TODOS LOS LAYOUTS BASE ===
layouts_base = df_schedule['ScheduleCD'].tolist()
for layout in layouts_base:
    expandir(layout, 0)

# === CONVERTIR A DATAFRAME Y ENRIQUECER ===
df_rel = pd.DataFrame(relaciones, columns=['Origen', 'Relacion', 'Destino'])
df_rel = df_rel.merge(df_schedule.add_prefix('Origen_'), left_on='Origen', right_on='Origen_ScheduleCD', how='left')
df_rel = df_rel.merge(df_schedule.add_prefix('Destino_'), left_on='Destino', right_on='Destino_ScheduleCD', how='left')
df_rel = df_rel.merge(df_matriz.add_prefix('M_'), left_on='Origen', right_on='M_ScheduleCD', how='left')

# === EXPORTACIÓN CON FECHA Y LOG ===
os.makedirs("output", exist_ok=True)
os.makedirs("data", exist_ok=True)

fecha_actual = datetime.now()
nombre_archivo = f"arbol_dependencias_matriz_{fecha_actual.strftime('%Y%m%d_%H%M%S')}.csv"
ruta_salida = os.path.join("output", nombre_archivo)
df_rel.to_csv(ruta_salida, sep='}<', index=False, encoding='utf-8-sig')

# === LOG DE EJECUCIÓN ===
log_path = os.path.join("data", "log_web_execution.txt")
with open(log_path, "a", encoding="utf-8") as f:
    f.write(f"\n==============================\n")
    f.write(f"🕒 Fecha ejecución: {fecha_actual.strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"📄 Archivo generado: {nombre_archivo}\n")
    f.write(f"🔢 Total relaciones generadas: {len(df_rel)}\n")

print(f"\n✅ Archivo generado: {ruta_salida}")
