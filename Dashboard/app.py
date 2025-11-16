import streamlit as st
from jwt_utils import verify_token
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json

st.set_page_config(
    page_title="Dashboard Operador - GAMC",
    page_icon="📟",
    layout="wide"
)

# NOTA: Se eliminó el CSS que ocultaba la barra lateral (sidebar)
# porque el dashboard la necesita para el menú de navegación.

# ================================
# 🔐 VALIDACIÓN DE SESIÓN Y ROL
# ================================
if "logged" not in st.session_state:
    st.switch_page("pages/auth_app.py")

if "token" not in st.session_state:
    st.session_state.clear()
    st.switch_page("pages/auth_app.py")

decoded = verify_token(st.session_state.get("token"))
if decoded is None:
    st.session_state.clear()
    st.switch_page("pages/auth_app.py")

if decoded["role"] != "operador":
    st.error("Acceso exclusivo para operadores")
    st.stop()

# ================================
# 🔘 BOTÓN DE LOGOUT
# ================================
st.markdown("""
<style>
.top-bar { display:flex; justify-content:flex-end; }
.top-bar button { background:#e74c3c; color:white; border-radius:8px; }
</style>
""", unsafe_allow_html=True)

top1, top2 = st.columns([8,2])
with top2:
    st.markdown('<div class="top-bar">', unsafe_allow_html=True)
    if st.button("🔒 Cerrar sesión"):
        st.session_state.clear()
        st.switch_page("pages/auth_app.py")
    st.markdown('</div>', unsafe_allow_html=True)

# ================================
# 📟 CONTENIDO DEL DASHBOARD DEL OPERADOR
# ================================
st.title("📟 Panel Operador")

# --- CONFIGURACIÓN KAFKA ---
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "datos_sensores"

# --- FUNCIONES ---
@st.cache_data(ttl=5)  # Recarga cada 5 segundos
def leer_datos_kafka(broker, max_mensajes=200):
    """
    Lee los últimos mensajes de Kafka desde el topic configurado.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[broker],
            auto_offset_reset='earliest',
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
            df = pd.json_normalize(registros)
            df['time'] = pd.to_datetime(df['time'], format='mixed', utc=True)
            df = df.sort_values('time')
            return df
        else:
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al conectar con Kafka: {e}")
        return pd.DataFrame()

# --- SIDEBAR (MENÚ DE NAVEGACIÓN) ---
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/4149/4149670.png", width=110)
st.sidebar.title(f"Bienvenido, {decoded.get('username', 'operador')}")
menu = st.sidebar.radio(
    "📊 Selecciona una sección",
    ["Calidad del Aire (EM500)", "Calidad del Sonido (WS302)", "Sensores Soterrados (EM310)"]
)

# --- CARGA DE DATOS ---
df = leer_datos_kafka(KAFKA_BROKER)

if df.empty:
    st.warning("No se han recibido datos de los sensores. Esperando nuevos eventos de Kafka...")
else:
    # --- SECCIÓN: CALIDAD DEL AIRE ---
    if menu == "Calidad del Aire (EM500)":
        st.markdown("## 🌫️ Calidad del Aire - EM500")
        if 'object.co2' in df.columns:
            df_aire = df.dropna(subset=['object.co2'])
            if not df_aire.empty:
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("CO₂ Promedio (ppm)", f"{df_aire['object.co2'].mean():.1f}")
                temp = df_aire['object.temperature'].mean() if 'object.temperature' in df_aire.columns else 0
                col2.metric("Temperatura (°C)", f"{temp:.1f}")
                hum = df_aire['object.humidity'].mean() if 'object.humidity' in df_aire.columns else 0
                col3.metric("Humedad (%)", f"{hum:.1f}")
                pres = df_aire['object.pressure'].mean() if 'object.pressure' in df_aire.columns else 0
                col4.metric("Presión (hPa)", f"{pres:.1f}")

                st.markdown("### 📈 Evolución temporal del CO₂")
                fig_co2 = px.line(df_aire, x='time', y='object.co2', title="CO₂ (ppm)", color_discrete_sequence=['#2196f3'])
                st.plotly_chart(fig_co2, use_container_width=True)
            else:
                st.warning("Se encontraron columnas de CO2, pero los datos están vacíos.")
        else:
            st.info("⏳ No se detectaron datos de Calidad del Aire (CO2) en los últimos mensajes recibidos.")

    # --- SECCIÓN: CALIDAD DEL SONIDO ---
    elif menu == "Calidad del Sonido (WS302)":
        st.markdown("## 🔊 Calidad del Sonido - WS302")
        col_ruido = 'object.LAeq' if 'object.LAeq' in df.columns else None

        if col_ruido:
            df_sound = df.dropna(subset=[col_ruido])
            if not df_sound.empty:
                col1, col2, col3 = st.columns(3)
                col1.metric("Ruido Promedio (dB)", f"{df_sound[col_ruido].mean():.1f}")
                col2.metric("Nivel Máximo (dB)", f"{df_sound[col_ruido].max():.1f}")
                bat_col = next((col for col in df_sound.columns if 'battery' in col.lower()), None)
                bat = df_sound[bat_col].mean() if bat_col else 0
                col3.metric("Batería Promedio", f"{bat:.1f}")

                st.markdown("### 🔊 Evolución del ruido")
                fig_noise = px.line(df_sound, x='time', y=col_ruido, title="Nivel de Ruido (dB)", color_discrete_sequence=['#0077b6'])
                st.plotly_chart(fig_noise, use_container_width=True)
            else:
                st.warning(f"Se encontró la columna '{col_ruido}', pero no tiene datos válidos.")
        else:
            st.info("🔇 No se detectaron datos de Sonido en este momento.")

    # --- SECCIÓN: SENSORES SOTERRADOS ---
    elif menu == "Sensores Soterrados (EM310)":
        st.markdown("## 🌱 Sensores Soterrados - EM310")
        if 'object.distance' in df.columns:
            df_sot = df.dropna(subset=['object.distance'])
            if not df_sot.empty:
                col1, col2, col3 = st.columns(3)
                col1.metric("Distancia Promedio (cm)", f"{df_sot['object.distance'].mean():.1f}")
                bat_col = next((col for col in df_sot.columns if 'battery' in col.lower()), None)
                bat = df_sot[bat_col].mean() if bat_col else 0
                col2.metric("Batería Promedio", f"{bat:.1f}")
                col3.metric("Eventos Registrados", len(df_sot))

                st.markdown("### 📊 Evolución de distancia")
                fig_dist = px.line(df_sot, x='time', y='object.distance', title="Distancia (cm)", color_discrete_sequence=['#2a9d8f'])
                st.plotly_chart(fig_dist, use_container_width=True)

                if 'object.status' in df_sot.columns:
                    st.markdown("### 📍 Estado de los sensores")
                    df_status = df_sot.dropna(subset=['object.status'])
                    if not df_status.empty:
                        fig_status = px.pie(df_status, names='object.status', title="Estado de los Sensores", color_discrete_sequence=px.colors.qualitative.Safe)
                        st.plotly_chart(fig_status, use_container_width=True)
            else:
                 st.warning("Llegaron columnas de distancia, pero los valores están vacíos.")
        else:
            st.info("🚜 No se detectaron datos de Sensores Soterrados en este momento.")
