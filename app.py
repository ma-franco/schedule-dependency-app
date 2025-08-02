# app.py
import streamlit as st
import pandas as pd
from genarbol_logic import generar_arbol_dependencias

# --- Título y estilo ---
st.set_page_config(page_title="Árbol de Dependencias", layout="wide")
st.title("🌳 Árbol de Dependencias de Layouts A")
st.markdown("Ingrese el **NombreLayout** base para generar el árbol completo de dependencias. A")

# --- Input ---
layout_base = st.text_input("🔎 Nombre del Layout base", "")

# --- Acción principal ---
if layout_base:
    with st.spinner("⏳ Generando árbol de dependencias..."):
        try:
            df_resultado = generar_arbol_dependencias(layout_base.strip())
            st.success(f"✅ Resultado generado para '{layout_base.upper()}' ({len(df_resultado)} registros)")
            st.dataframe(df_resultado, use_container_width=True)

            # --- Exportación ---
            st.markdown("### 📤 Exportar resultado")
            sep_opciones = {
                "Pipe (`|`)": "|",
                "Punto y coma (`;`)": ";",
                "Especial (`}` y `<`) - reemplazado por `}`": "}"
            }
            sep_label = st.selectbox("Selecciona delimitador para exportar:", list(sep_opciones.keys()))
            sep = sep_opciones[sep_label]
            nombre_archivo = f"arbol_{layout_base.strip().upper()}.csv"
            csv = df_resultado.to_csv(index=False, sep=sep, encoding="utf-8-sig")
            st.download_button("⬇️ Descargar CSV", csv, file_name=nombre_archivo, mime="text/csv")

        except Exception as e:
            st.error(f"❌ Error: {str(e)}")


