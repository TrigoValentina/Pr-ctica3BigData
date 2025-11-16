import streamlit as st
from jwt_utils import verify_token
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import json
import datetime

st.set_page_config(page_title="Panel Ejecutivo", page_icon="", layout="wide")

# =============================
# CONFIGURACIÓN KAFKA
# =============================
KAFKA_BROKER = "localhost:29092"
TOPIC_NAME = "datos_sensores"

# =============================
# FUNCIONES
# =============================
@st.cache_data(ttl=60)  # Cache más largo para análisis
def leer_datos_kafka(broker, max_mensajes=2000):
    """Lee una mayor cantidad de mensajes de Kafka para análisis histórico."""
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[broker],
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000
        )
        registros = [msg.value for i, msg in enumerate(consumer) if i < max_mensajes]
        consumer.close()

        if registros:
            df = pd.json_normalize(registros)
            df['time'] = pd.to_datetime(df['time'])
            # Para análisis, es mejor tenerlo ordenado de forma ascendente
            return df.sort_values('time', ascending=True)
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error al conectar con Kafka: {e}")
        return pd.DataFrame()



# ================================
# VALIDACIÓN DE SESIÓN SEGURA
# ================================
session = st.session_state

logged = session.get("logged", False)
token = session.get("token", None)

# 1️⃣  Si no está logeado → redirigir
if not logged or token is None:
    st.switch_page("pages/auth_app.py")
    st.stop()

# 2️⃣ Intentar decodificar token
decoded = verify_token(token)

# Token inválido o expirado
if decoded is None:
    session.clear()
    st.switch_page("pages/auth_app.py")
    st.stop()

# 3️⃣ Validar rol
role_requerido = "ejecutivo"   # CAMBIAR por operador / ejecutivo
if decoded.get("role") != role_requerido:
    st.error("Acceso denegado.")
    st.stop()

# ================================
# LAYOUT Y ESTILOS
# ================================
st.markdown("""
<style>
    /* Ocultar sidebar por defecto */
    section[data-testid="stSidebar"] { display: none; }
    /* Botón de logout */
    .stButton>button {
        background-color: #e74c3c;
        color: white;
        border-radius: 8px;
        border: none;
    }
    /* Títulos del resumen estadístico */
    .resumen-title {
        font-size: 1.5rem;
        font-weight: bold;
        margin-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

# ================================
# TÍTULO Y LOGOUT
# ================================
col_titulo, col_logout = st.columns([0.85, 0.15])
with col_titulo:
    st.title("📈 Panel de Análisis Ejecutivo")
with col_logout:
    if st.button("Cerrar sesión", use_container_width=True):
        session.clear()
        st.switch_page("pages/auth_app.py")

st.markdown("---")

# ================================
# CARGA DE DATOS
# ================================
df_full = leer_datos_kafka(KAFKA_BROKER)

if df_full.empty:
    st.warning("No se encontraron datos para el análisis. Esperando mensajes...")
    st.stop()

# Usaremos CO2 como la métrica principal para este dashboard
df_main = df_full[df_full['object.co2'].notna()].copy()

# ================================
# CONTROLES DE FILTRO
# ================================
st.subheader("Filtros de Análisis")

c1, c2, c3 = st.columns([1, 1, 2])
with c1:
    fecha_inicio = st.date_input("Desde", df_main['time'].min().date())
with c2:
    fecha_fin = st.date_input("Hasta", df_main['time'].max().date())

# Filtrar el DataFrame por el rango de fechas seleccionado
df_filtered = df_main[(df_main['time'].dt.date >= fecha_inicio) & (df_main['time'].dt.date <= fecha_fin)]

# ================================
# LAYOUT PRINCIPAL: GRÁFICO Y RESUMEN
# ================================
col_main, col_stats = st.columns([0.7, 0.3])

with col_main:
    st.subheader("Evolución de la Métrica Principal (CO₂)")
    if not df_filtered.empty:
        # Usamos el DataFrame filtrado por fecha para el gráfico
        fig = px.line(df_filtered, x='time', y='object.co2', title="Niveles de CO₂ en el tiempo")
        fig.update_layout(
            yaxis_title="CO₂ (ppm)",
            xaxis_title="Fecha",
            yaxis=dict(range=[100, 1000]) # Rango Y como se pidió
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No hay datos disponibles para el rango de fechas seleccionado.")

with col_stats:
    st.markdown("<div class='resumen-title'>Resumen Estadístico</div>", unsafe_allow_html=True)
    
    if not df_filtered.empty:
        min_val = df_filtered['object.co2'].min()
        max_val = df_filtered['object.co2'].max()
        mean_val = df_filtered['object.co2'].mean()
        median_val = df_filtered['object.co2'].median()

        st.metric(label="Mínimo", value=f"{min_val:.2f} ppm")
        st.metric(label="Máximo", value=f"{max_val:.2f} ppm")
        st.metric(label="Promedio", value=f"{mean_val:.2f} ppm")
        st.metric(label="Mediana", value=f"{median_val:.2f} ppm")
    else:
        st.info("Sin datos para calcular estadísticas.")
