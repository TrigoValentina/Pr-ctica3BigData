# dashboards/dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json

# =============================
# CONFIGURACIÓN DE PÁGINA
# =============================
st.set_page_config(
    page_title="Dashboard Ambiental - GAMC",
    layout="wide"
)

# =============================
# CONFIGURACIÓN KAFKA
# =============================
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "datos_sensores"

# =============================
# FUNCIONES
# =============================
@st.cache_data(ttl=5)  # Recarga cada 5 segundos
def leer_datos_kafka(broker, max_mensajes=100):
    """
    Lee los últimos mensajes de Kafka desde el topic configurado.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[broker],
            auto_offset_reset='earliest',  # Desde el primer mensaje disponible
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        registros = []
        for i, msg in enumerate(consumer):
            registros.append(msg.value)
            if i >= max_mensajes - 1:
                break
        consumer.close()

        if registros:
            # Normaliza JSON anidado
            df = pd.json_normalize(registros)
            # Convertir columna time a datetime
            df['time'] = pd.to_datetime(df['time'])
            # Ordenar por tiempo
            df = df.sort_values('time')
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al conectar con Kafka: {e}")
        return pd.DataFrame()

# =============================
# SIDEBAR
# =============================
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/4149/4149670.png", width=110)
menu = st.sidebar.radio(
    "📊 Selecciona una sección",
    ["Calidad del Aire (EM500)", "Calidad del Sonido (WS302)", "Sensores Soterrados (EM310)"]
)

# =============================
# CARGA DE DATOS
# =============================
df = leer_datos_kafka(KAFKA_BROKER, max_mensajes=200)

if df.empty:
    st.warning("No hay datos recientes de Kafka. Espera unos segundos...")
else:
    # =============================
    # CALIDAD DEL AIRE
    # =============================
    if menu == "Calidad del Aire (EM500)":
        st.markdown("## 🌫️ Calidad del Aire - EM500")

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("CO₂ Promedio (ppm)", f"{df['object.co2'].mean():.1f}")
        col2.metric("Temperatura (°C)", f"{df['object.temperature'].mean():.1f}")
        col3.metric("Humedad (%)", f"{df['object.humidity'].mean():.1f}")
        col4.metric("Presión (hPa)", f"{df['object.pressure'].mean():.1f}")

        st.markdown("### 📈 Evolución temporal")
        fig_co2 = px.line(df, x='time', y='object.co2', title="CO₂ (ppm)", color_discrete_sequence=['#2196f3'])
        st.plotly_chart(fig_co2, use_container_width=True)

        fig_temp = px.line(df, x='time', y='object.temperature', title="Temperatura (°C)", color_discrete_sequence=['#e76f51'])
        st.plotly_chart(fig_temp, use_container_width=True)

        fig_hum = px.area(df, x='time', y='object.humidity', title="Humedad (%)", color_discrete_sequence=['#2a9d8f'])
        st.plotly_chart(fig_hum, use_container_width=True)

        fig_pres = px.line(df, x='time', y='object.pressure', title="Presión (hPa)", color_discrete_sequence=['#6a4c93'])
        st.plotly_chart(fig_pres, use_container_width=True)

    # =============================
    # CALIDAD DEL SONIDO
    # =============================
    elif menu == "Calidad del Sonido (WS302)":
        st.markdown("## 🔊 Calidad del Sonido - WS302")

        # Algunos CSV usan 'object.noise' o 'object.laeq'
        col_ruido = 'object.noise' if 'object.noise' in df.columns else 'object.laeq'

        col1, col2, col3 = st.columns(3)
        col1.metric("Ruido Promedio (dB)", f"{df[col_ruido].mean():.1f}")
        col2.metric("Nivel Máximo (dB)", f"{df[col_ruido].max():.1f}")
        if 'object.battery' in df.columns:
            col3.metric("Batería Promedio (%)", f"{df['object.battery'].mean():.1f}")
        else:
            col3.metric("Batería Promedio (%)", "N/A")

        st.markdown("### 🔊 Evolución del ruido")
        fig_noise = px.line(df, x='time', y=col_ruido, title="Nivel de Ruido (dB)", color_discrete_sequence=['#0077b6'])
        st.plotly_chart(fig_noise, use_container_width=True)

    # =============================
    # SENSORES SOTERRADOS
    # =============================
    elif menu == "Sensores Soterrados (EM310)":
        st.markdown("## 🌱 Sensores Soterrados - EM310")

        col1, col2, col3 = st.columns(3)
        col1.metric("Distancia Promedio (cm)", f"{df['object.distance'].mean():.1f}")
        col2.metric("Batería Promedio (V)", f"{df['object.battery'].mean():.1f}")
        col3.metric("Eventos Registrados", len(df))

        st.markdown("### 📊 Evolución de distancia")
        fig_dist = px.line(df, x='time', y='object.distance', title="Distancia (cm)", color_discrete_sequence=['#2a9d8f'])
        st.plotly_chart(fig_dist, use_container_width=True)

        if 'object.status' in df.columns:
            st.markdown("### 📍 Estado de los sensores")
            fig_status = px.pie(df, names='object.status', title="Estado de los Sensores", color_discrete_sequence=px.colors.qualitative.Safe)
            st.plotly_chart(fig_status, use_container_width=True)
