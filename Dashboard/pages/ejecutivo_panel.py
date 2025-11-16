import streamlit as st
from jwt_utils import verify_token
from pymongo import MongoClient
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta

st.set_page_config(page_title="Panel Ejecutivo", page_icon="", layout="wide")

# Ocultar sidebar
st.markdown("""
<style>
section[data-testid="stSidebar"] { display:none; }
div[data-testid="stAppViewContainer"] { margin-left:0 !important; }
</style>
""", unsafe_allow_html=True)

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

st.title("📈 Panel Ejecutivo")
st.markdown("Análisis de tendencias y KPIs históricos.")

# ================================
# ⚙️ CONFIGURACIÓN Y CONEXIÓN A MONGODB
# ================================
MONGO_URI = "mongodb+srv://emergentes118_db_user:Womita14@cluster0.xcvehjm.mongodb.net/?retryWrites=true&w=majority"
MONGO_DB = "gamc_db"
MONGO_COLLECTION = "sensores"

@st.cache_resource
def get_mongo_client():
    """Crea y cachea la conexión a MongoDB."""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.server_info() # Fuerza la conexión para probarla
        return client
    except Exception as e:
        st.error(f"No se pudo conectar a MongoDB: {e}")
        return None

@st.cache_data(ttl=300) # Cache por 5 minutos
def fetch_data_from_mongo(_client, start_date, end_date):
    """Obtiene datos de MongoDB para un rango de fechas."""
    if _client is None:
        return pd.DataFrame()
    
    db = _client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    
    query = {"time": {"$gte": start_date, "$lte": end_date}}
    cursor = collection.find(query)
    df = pd.DataFrame(list(cursor))

    if not df.empty:
        df['time'] = pd.to_datetime(df['time'])
        # Convertir columnas numéricas, forzando errores a NaN
        for col in ['co2', 'LAeq', 'distance', 'temperature', 'humidity']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

# ================================
# 🎨 UI: FILTROS Y CARGA DE DATOS
# ================================
client = get_mongo_client()

st.sidebar.header("Filtros")
end_date = datetime.now()
start_date = st.sidebar.date_input("Fecha de inicio", end_date - timedelta(days=30))
end_date_ui = st.sidebar.date_input("Fecha de fin", end_date)

# Convertir a datetime para la query
start_datetime = datetime.combine(start_date, datetime.min.time())
end_datetime = datetime.combine(end_date_ui, datetime.max.time())

df = fetch_data_from_mongo(client, start_datetime, end_datetime)

if client is None:
    st.stop()

if df.empty:
    st.warning("No se encontraron datos históricos para el período seleccionado.")
    st.stop()

# ================================
# 📊 VISUALIZACIONES POR TABS
# ================================
tab1, tab2, tab3 = st.tabs(["🌫️ Calidad del Aire", "🔊 Ruido Ambiental", "🌱 Gestión de Residuos"])

with tab1:
    st.header("Análisis de Calidad del Aire (CO₂)")
    df_aire = df.dropna(subset=['co2'])
    if not df_aire.empty:
        avg_co2 = df_aire['co2'].mean()
        st.metric("CO₂ Promedio (ppm)", f"{avg_co2:.1f}")

        st.subheader("Tendencia de CO₂ Diario")
        daily_avg_co2 = df_aire.set_index('time').resample('D')['co2'].mean().reset_index()
        fig_co2_trend = px.line(daily_avg_co2, x='time', y='co2', title="Promedio Diario de CO₂", labels={'time': 'Fecha', 'co2': 'CO₂ (ppm)'})
        st.plotly_chart(fig_co2_trend, use_container_width=True)
    else:
        st.info("No hay datos de CO₂ para este período.")

with tab2:
    st.header("Análisis de Ruido Ambiental")
    df_ruido = df.dropna(subset=['LAeq'])
    if not df_ruido.empty:
        avg_ruido = df_ruido['LAeq'].mean()
        st.metric("Ruido Promedio (dB)", f"{avg_ruido:.1f}")

        st.subheader("Nivel de Ruido Promedio por Hora del Día")
        df_ruido['hour'] = df_ruido['time'].dt.hour
        hourly_avg_noise = df_ruido.groupby('hour')['LAeq'].mean().reset_index()
        fig_noise_hourly = px.bar(hourly_avg_noise, x='hour', y='LAeq', title="Patrón de Ruido Diario", labels={'hour': 'Hora del Día', 'LAeq': 'Ruido Promedio (dB)'})
        st.plotly_chart(fig_noise_hourly, use_container_width=True)
    else:
        st.info("No hay datos de Ruido para este período.")

with tab3:
    st.header("Análisis de Sensores Soterrados")
    df_sot = df.dropna(subset=['distance', 'status'])
    if not df_sot.empty:
        # Para el estado, tomamos el último registro de cada sensor
        latest_status = df_sot.sort_values('time').drop_duplicates(subset=['device_name'], keep='last')
        
        st.subheader("Estado Actual de Contenedores")
        status_counts = latest_status['status'].value_counts().reset_index()
        status_counts.columns = ['status', 'count']
        fig_status_pie = px.pie(status_counts, names='status', values='count', title="Distribución de Estado de Contenedores")
        st.plotly_chart(fig_status_pie, use_container_width=True)
    else:
        st.info("No hay datos de sensores soterrados para este período.")
