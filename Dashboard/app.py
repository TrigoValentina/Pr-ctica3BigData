import streamlit as st
import pandas as pd
import plotly.express as px
import os

# =============================
# CONFIGURACIÓN GENERAL
# =============================
st.set_page_config(
    page_title="Dashboard Ambiental - Big Data",
    page_icon="",
    layout="wide"
)

DATA_PATH = "../data"

@st.cache_data
def load_data(filename):
    path = os.path.join(DATA_PATH, filename)
    df = pd.read_csv(path)
    return df

# Sidebar
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/4149/4149670.png", width=110)
menu = st.sidebar.radio(
    "Selecciona una sección",
    ["Calidad del Aire (EM500)", "Calidad del Sonido (WS302)", "Sensores Soterrados (EM310)"]
)

# ============================================================
# CALIDAD DEL AIRE - EM500
# ============================================================
if menu == "Calidad del Aire (EM500)":
    st.markdown("## Calidad del Aire - Sensor EM500")
    df = load_data("EM500-CO2-915M nov 2024.csv")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("CO₂ Promedio (ppm)", f"{df['object.co2'].mean():.1f}")
    col2.metric("Temperatura (°C)", f"{df['object.temperature'].mean():.1f}")
    col3.metric("Humedad (%)", f"{df['object.humidity'].mean():.1f}")
    col4.metric("Presión (hPa)", f"{df['object.pressure'].mean():.1f}")

    st.markdown("### Evolución temporal de variables")
    col1, col2 = st.columns(2)

    with col1:
        fig_co2 = px.line(df, x="time", y="object.co2", title="Concentración de CO₂ (ppm)", color_discrete_sequence=["#2196f3"])
        st.plotly_chart(fig_co2, use_container_width=True)

    with col2:
        fig_temp = px.line(df, x="time", y="object.temperature", title="Temperatura Ambiental (°C)", color_discrete_sequence=["#e76f51"])
        st.plotly_chart(fig_temp, use_container_width=True)

    col3, col4 = st.columns(2)
    with col3:
        fig_hum = px.area(df, x="time", y="object.humidity", title="Humedad Relativa (%)", color_discrete_sequence=["#2a9d8f"])
        st.plotly_chart(fig_hum, use_container_width=True)
    with col4:
        fig_pres = px.line(df, x="time", y="object.pressure", title="Presión Atmosférica (hPa)", color_discrete_sequence=["#6a4c93"])
        st.plotly_chart(fig_pres, use_container_width=True)

# ============================================================
# CALIDAD DEL SONIDO - WS302
# ============================================================
elif menu == "Calidad del Sonido (WS302)":
    st.markdown("## Calidad del Sonido - Sensor WS302")
    df = load_data("WS302-915M SONIDO NOV 2024.csv")

    # Normalizar columnas para evitar errores por mayúsculas
    df.columns = df.columns.str.strip().str.lower()

    # Buscar columnas relevantes sin importar formato exacto
    col_ruido = next((c for c in df.columns if "laeq" in c or "leq" in c or "noise" in c), None)
    col_max = next((c for c in df.columns if "latmax" in c or "max" in c), None)
    col_bateria = next((c for c in df.columns if "battery" in c), None)

    if not col_ruido:
        st.error("No se encontró ninguna columna de ruido (LAeq, Leq o noise).")
    else:
        # Métricas
        col1, col2, col3 = st.columns(3)
        col1.metric("Ruido Promedio (dB)", f"{df[col_ruido].mean():.1f}")
        if col_max:
            col2.metric("Nivel Máximo (dB)", f"{df[col_max].max():.1f}")
        else:
            col2.metric("Nivel Máximo (dB)", "No disponible")
        if col_bateria:
            col3.metric("Batería Promedio (%)", f"{df[col_bateria].mean():.1f}")
        else:
            col3.metric("Batería Promedio (%)", "N/A")

        # Gráfico de distribución
        st.markdown("### Distribución de niveles de ruido")
        fig1 = px.histogram(
            df,
            x=col_ruido,
            nbins=50,
            color_discrete_sequence=["#0077b6"],
            title=f"Frecuencia de Niveles de Ruido ({col_ruido})"
        )
        st.plotly_chart(fig1, use_container_width=True)

        # Gráfico temporal
        st.markdown("### Evolución del ruido en el tiempo")
        fig2 = px.line(
            df,
            x="time",
            y=col_ruido,
            color_discrete_sequence=["#00b4d8"],
            title=f"Tendencia del Nivel de Ruido ({col_ruido}) en el Tiempo"
        )
        st.plotly_chart(fig2, use_container_width=True)

        # Comparativa si hay datos de máximo
        if col_max:
            st.markdown("### Comparativa entre niveles promedio y máximo")
            fig3 = px.line(
                df,
                x="time",
                y=[col_ruido, col_max],
                labels={"value": "dB", "variable": "Indicador"},
                title="Comparativa: Nivel Promedio vs Nivel Máximo"
            )
            st.plotly_chart(fig3, use_container_width=True)


# ============================================================
# SENSORES SOTERRADOS - EM310
# ============================================================
elif menu == "Sensores Soterrados (EM310)":
    st.markdown("## Sensores Soterrados - EM310")
    df = load_data("EM310-UDL-915M soterrados nov 2024.csv")

    col1, col2, col3 = st.columns(3)
    col1.metric("Distancia Promedio (cm)", f"{df['object.distance'].mean():.1f}")
    col2.metric("Batería Promedio (V)", f"{df['object.battery'].mean():.1f}")
    col3.metric("Eventos Registrados", len(df))

    st.markdown("### Nivel de distancia en el tiempo")
    fig1 = px.line(df, x="time", y="object.distance", title="Evolución de la Distancia Detectada", color_discrete_sequence=["#2a9d8f"])
    st.plotly_chart(fig1, use_container_width=True)

    st.markdown("### Distribución de sensores por estado")
    if 'object.status' in df.columns:
        fig2 = px.pie(df, names='object.status', title="Estado de los Sensores", color_discrete_sequence=px.colors.qualitative.Safe)
        st.plotly_chart(fig2, use_container_width=True)
