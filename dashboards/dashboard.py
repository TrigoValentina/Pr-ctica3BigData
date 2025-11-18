# dashboards/dashboard.py
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import mysql.connector
from pymongo import MongoClient
import logging
from datetime import datetime, timedelta
import numpy as np
import bcrypt
from supabase import create_client, Client
import dashboards.dashboard2 as dashboard2

# =============================
# CONFIGURACIÓN DE PÁGINA
# =============================
st.set_page_config(
    page_title="Dashboard Ambiental - GAMC",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# =============================
# CONFIGURACIÓN DE LOGGING
# =============================
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================
# 🔗 SUPABASE (USUARIOS + LOGS)
# =============================
SUPABASE_URL = "https://ugqhpqllxrcjyusslasg.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InVncWhwcWxseHJjanl1c3NsYXNnIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjMyNDE0NDgsImV4cCI6MjA3ODgxNzQ0OH0.bwVIZf6bCqL1cuYZwFvwgysLZvDv2LzyvgxcLEpDA0U"  # anon key

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


def get_user_by_username(username: str):
    """Busca usuario en Supabase por username."""
    try:
        res = (
            supabase
            .table("app_users")
            .select("*")
            .eq("username", username)
            .execute()
        )
        if res.data:
            return res.data[0]
        return None
    except Exception as e:
        logger.error(f"Error obteniendo usuario '{username}' desde Supabase: {e}")
        return None


def log_action(user_id, action: str, level: str = "info", data: dict | None = None):
    """Registra acción en app_logs de Supabase."""
    try:
        payload = {
            "user_id": user_id,
            "action": action,
            "level": level,
            "data": data or {},
        }
        supabase.table("app_logs").insert(payload).execute()
    except Exception as e:
        logger.error(f"Error registrando log en Supabase: {e}")


# =============================
# CONTROL DE ACCESO (LOGIN)
# =============================

# Inicializar estado de sesión
if "is_authenticated" not in st.session_state:
    st.session_state["is_authenticated"] = False
if "usuario_actual" not in st.session_state:
    st.session_state["usuario_actual"] = None
if "user_role" not in st.session_state:
    st.session_state["user_role"] = None
if "user_id" not in st.session_state:
    st.session_state["user_id"] = None


def mostrar_login():
    st.title("🔐 Sistema GAMC - Inicio de Sesión")
    st.markdown("Por favor ingresa tus credenciales para acceder al dashboard.")

    with st.form("login_form"):
        usuario = st.text_input("Usuario")
        contrasena = st.text_input("Contraseña", type="password")
        recordar = st.checkbox(
            "Recordarme",
            value=True,
            help="Mantener la sesión activa mientras el navegador esté abierto."
        )
        submit = st.form_submit_button("Ingresar")

    if submit:
        if not usuario or not contrasena:
            st.error("Por favor ingresa usuario y contraseña.")
            return

        user = get_user_by_username(usuario)

        if not user:
            st.error("Usuario o contraseña incorrectos.")
            return

        # Validar que esté activo (si usas is_active)
        if user.get("is_active") is False:
            st.error("Tu usuario está inactivo. Contacta con el administrador.")
            return

        password_hash = user.get("password_hash")

        if not password_hash:
            st.error("Tu usuario no tiene contraseña configurada. Contacta al administrador.")
            return

        try:
            ok = bcrypt.checkpw(
                contrasena.encode("utf-8"),
                password_hash.encode("utf-8")
            )
        except Exception:
            ok = False

        if not ok:
            st.error("Usuario o contraseña incorrectos.")
            return

        # ✅ Login correcto: guardar sesión
        st.session_state["is_authenticated"] = True
        st.session_state["usuario_actual"] = user["username"]
        st.session_state["user_role"] = user.get("role", "sin rol")
        st.session_state["user_id"] = user.get("id")

        # Registrar en logs
        if st.session_state["user_id"]:
            log_action(st.session_state["user_id"], "login", "info", {"username": user["username"]})


def boton_logout():
    """Botón para cerrar sesión, en el sidebar."""
    if st.sidebar.button("🚪 Cerrar sesión"):
        uid = st.session_state.get("user_id")
        if uid:
            log_action(uid, "logout", "info", {})
        st.session_state.clear()
        st.success("Sesión cerrada.")
        st.rerun()


# Si no está autenticado → mostrar login y cortar
if not st.session_state["is_authenticated"]:
    mostrar_login()
    st.stop()
# =============================
# REDIRECCIÓN POR ROL
# =============================
rol = st.session_state["user_role"]

if rol == "ejecutivo":
    dashboard2.main()
    st.stop()

# operador y otros roles continúan al dashboard normal

# =============================
# CONFIGURACIÓN DE BASES DE DATOS (MySQL + Mongo)
# =============================
# MySQL
DB_HOST = "localhost"
DB_PORT = 3307  # Puerto mapeado en Docker
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"

# MongoDB Atlas (si lo usas en algún momento)
MONGO_ATLAS_URI = "mongodb+srv://jg012119:cEfOpibMb2iFfrCs@cluster0.oyerk.mongodb.net/emergentETLVALENTINA?retryWrites=true&w=majority&appName=Cluster0"
MONGO_COLLECTION = "sensores"

# =============================
# FUNCIONES DE CONEXIÓN MySQL
# =============================
def get_mysql_connection():
    """Crea una conexión a MySQL (sin cache para evitar problemas de conexión cerrada)"""
    try:
        logger.info(f"🔌 Intentando conectar a MySQL: {DB_HOST}:{DB_PORT}/{DB_NAME}")
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            autocommit=True
        )
        logger.info("✅ Conexión a MySQL establecida correctamente")
        return conn
    except Exception as e:
        logger.error(f"❌ Error conectando a MySQL: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def leer_datos_mysql(tabla, use_cache=True):
    """Lee datos de una tabla específica en MySQL"""
    if use_cache:
        return _leer_datos_mysql_cached(tabla)
    else:
        return _leer_datos_mysql_directo(tabla)


@st.cache_data(ttl=60)  # Cache por 60 segundos
def _leer_datos_mysql_cached(tabla):
    """Versión con cache"""
    return _leer_datos_mysql_directo(tabla)


def _leer_datos_mysql_directo(tabla):
    """Lee datos directamente sin cache"""
    conn = None
    try:
        logger.info(f"📊 Intentando leer datos de la tabla: {tabla}")
        conn = get_mysql_connection()
        if conn is None:
            logger.error("❌ No se pudo establecer conexión a MySQL")
            st.error("❌ No se pudo conectar a MySQL. Verifica que el servicio esté corriendo.")
            return pd.DataFrame()
        
        if not conn.is_connected():
            logger.error("❌ La conexión a MySQL no está activa")
            st.error("❌ La conexión a MySQL no está activa. Intenta recargar.")
            return pd.DataFrame()
        
        logger.info(f"✅ Conexión establecida, ejecutando query...")
        query = f"SELECT * FROM `{tabla}` ORDER BY time DESC LIMIT 10000"
        df = pd.read_sql(query, conn)
        
        if not df.empty:
            logger.info(f"📈 Datos leídos: {len(df)} registros")
            logger.info(f"📋 Columnas encontradas: {list(df.columns)}")
            if 'time' in df.columns:
                df['time'] = pd.to_datetime(df['time'])
                df = df.sort_values('time')
            logger.info(f"✅ DataFrame preparado con {len(df)} filas")
            st.success(f"✅ {len(df)} registros cargados de la tabla {tabla}")
        else:
            logger.warning(f"⚠️ La tabla {tabla} está vacía")
            st.warning(f"⚠️ La tabla {tabla} está vacía")
        
        return df
    except Exception as e:
        logger.error(f"❌ Error leyendo datos de MySQL: {e}")
        import traceback
        error_trace = traceback.format_exc()
        logger.error(error_trace)
        st.error(f"❌ Error al leer datos: {e}")
        st.code(error_trace)
        return pd.DataFrame()
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
            logger.info("🔌 Conexión MySQL cerrada")


@st.cache_data(ttl=60)
def leer_todos_datos_mysql():
    """Lee datos de todas las tablas y los combina"""
    try:
        tablas = ['em310_soterrados', 'em500_co2', 'ws302_sonido', 'otros']
        dfs = []
        
        for tabla in tablas:
            df = leer_datos_mysql(tabla)
            if not df.empty:
                df['tipo_sensor'] = tabla
                dfs.append(df)
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error combinando datos: {e}")
        return pd.DataFrame()

# =============================
# FUNCIONES DE VISUALIZACIÓN
# =============================
def crear_grafico_evolucion_temporal(df, columna_y, titulo, color_by=None):
    """Crea un gráfico de línea temporal"""
    fig = px.line(
        df,
        x='time',
        y=columna_y,
        color=color_by if color_by else None,
        title=titulo,
        labels={
            'time': 'Fecha',
            columna_y: titulo.split('(')[-1].replace(')', '') if '(' in titulo else columna_y
        }
    )
    fig.update_layout(
        hovermode='x unified',
        xaxis_title="Fecha",
        yaxis_title=titulo,
        height=400
    )
    return fig


def crear_grafico_barras_promedio(df, columna_x, columna_y, titulo):
    """Crea un gráfico de barras con promedios"""
    promedios = df.groupby(columna_x)[columna_y].mean().reset_index()
    promedios = promedios.sort_values(columna_y, ascending=False)
    
    fig = px.bar(
        promedios,
        x=columna_x,
        y=columna_y,
        title=titulo,
        labels={columna_x: 'Sensor', columna_y: 'Promedio'},
        color=columna_x,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig.update_layout(
        xaxis_title="Sensor",
        yaxis_title="Nivel Promedio",
        height=400,
        showlegend=False
    )
    return fig


def crear_boxplot_distribucion(df, columna_x, columna_y, titulo):
    """Crea un box plot de distribución"""
    fig = px.box(
        df,
        x=columna_x,
        y=columna_y,
        title=titulo,
        labels={columna_x: 'Sensor', columna_y: 'Nivel'},
        color=columna_x,
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    fig.update_layout(
        xaxis_title="Sensor",
        yaxis_title="Nivel",
        height=400,
        showlegend=False
    )
    return fig


def crear_heatmap_hora_dia_semana(df, columna_valor):
    """Crea un heatmap de hora del día vs día de la semana"""
    df_heat = df.copy()
    df_heat['dia_semana'] = df_heat['time'].dt.day_name()
    df_heat['hora'] = df_heat['time'].dt.hour
    
    dias_orden = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dias_espanol = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    df_heat['dia_num'] = df_heat['dia_semana'].map({dia: i for i, dia in enumerate(dias_orden)})
    
    df_heat = df_heat[df_heat[columna_valor].notna() & df_heat['dia_num'].notna()]
    
    if df_heat.empty:
        return None
    
    heatmap_data = df_heat.groupby(['dia_num', 'hora'])[columna_valor].mean().reset_index()
    heatmap_pivot = heatmap_data.pivot(index='hora', columns='dia_num', values=columna_valor)
    horas_completas = pd.DataFrame({'hora': range(24)})
    dias_completos = list(range(7))
    
    heatmap_pivot = heatmap_pivot.reindex(range(24))
    
    for dia in dias_completos:
        if dia not in heatmap_pivot.columns:
            heatmap_pivot[dia] = np.nan
    
    heatmap_pivot = heatmap_pivot.reindex(columns=dias_completos)
    heatmap_pivot.columns = dias_espanol
    
    fig = px.imshow(
        heatmap_pivot,
        labels=dict(x="Día de la Semana", y="Hora del Día", color="LAeq Promedio"),
        title="1.4 Patrón de Ruido: Hora del Día vs. Día de la Semana",
        color_continuous_scale='Blues',
        aspect="auto",
        text_auto='.1f'
    )
    fig.update_layout(
        height=600,
        xaxis_title="Día de la Semana",
        yaxis_title="Hora del Día"
    )
    return fig

# =============================
# SIDEBAR
# =============================
st.sidebar.title("📊 Dashboard Ambiental")
st.sidebar.markdown("---")

user_name = st.session_state.get("usuario_actual", "Desconocido")
user_role = st.session_state.get("user_role", "sin rol")
st.sidebar.info(f"👤 Usuario: **{user_name}** \n\n🛡 Rol: **{user_role}**")

boton_logout()

menu = st.sidebar.radio(
    "Selecciona una sección",
    [
        "🔊 Calidad del Sonido (WS302)",
        "🌫️ Calidad del Aire (EM500)",
        "🌱 Sensores Soterrados (EM310)"
    ]
)

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Configuración")
auto_refresh = st.sidebar.checkbox("Auto-refrescar", value=False)
if auto_refresh:
    refresh_interval = st.sidebar.slider("Intervalo (segundos)", 5, 60, 10)
    st.sidebar.info(f"🔄 Actualizando cada {refresh_interval}s")

if st.sidebar.button("🔄 Limpiar Cache y Recargar"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.success("✅ Cache limpiado")
    st.rerun()

sin_cache = st.sidebar.checkbox("🚫 Deshabilitar cache (más lento pero siempre actualizado)", value=False)

# =============================
# CARGA DE DATOS
# =============================
st.title("📊 Dashboard Ambiental - GAMC")
st.markdown("---")

with st.spinner("Cargando datos..."):
    try:
        if menu == "🔊 Calidad del Sonido (WS302)":
            df = leer_datos_mysql('ws302_sonido', use_cache=not sin_cache)
        elif menu == "🌫️ Calidad del Aire (EM500)":
            df = leer_datos_mysql('em500_co2', use_cache=not sin_cache)
        elif menu == "🌱 Sensores Soterrados (EM310)":
            df = leer_datos_mysql('em310_soterrados', use_cache=not sin_cache)
        else:
            df = pd.DataFrame()
        
        if st.sidebar.checkbox("🔍 Mostrar información de debug", value=False):
            st.sidebar.write(f"**Tabla consultada:** {menu}")
            st.sidebar.write(f"**Registros encontrados:** {len(df)}")
            if not df.empty:
                st.sidebar.write(f"**Columnas:** {list(df.columns)}")
                if 'time' in df.columns:
                    st.sidebar.write(f"**Primera fecha:** {df['time'].min()}")
                    st.sidebar.write(f"**Última fecha:** {df['time'].max()}")
    except Exception as e:
        st.error(f"Error al cargar datos: {e}")
        import traceback
        st.code(traceback.format_exc())
        df = pd.DataFrame()

# =============================
# CALIDAD DEL SONIDO (WS302)
# =============================
if menu == "🔊 Calidad del Sonido (WS302)":
    st.markdown("## 🔊 Calidad del Sonido - WS302")
    
    if df.empty:
        st.warning("⚠️ No hay datos disponibles para sensores de sonido.")
    else:
        if 'LAeq' in df.columns:
            df['LAeq'] = pd.to_numeric(df['LAeq'], errors='coerce')
        if 'LAI' in df.columns:
            df['LAI'] = pd.to_numeric(df['LAI'], errors='coerce')
        if 'LAImax' in df.columns:
            df['LAImax'] = pd.to_numeric(df['LAImax'], errors='coerce')
        
        df_sonido = df[df['LAeq'].notna()].copy()
        
        if df_sonido.empty:
            st.warning("⚠️ No hay datos válidos de LAeq.")
        else:
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("LAeq Promedio (dB)", f"{df_sonido['LAeq'].mean():.1f}")
            with col2:
                st.metric("LAeq Máximo (dB)", f"{df_sonido['LAeq'].max():.1f}")
            with col3:
                st.metric("LAeq Mínimo (dB)", f"{df_sonido['LAeq'].min():.1f}")
            with col4:
                st.metric("Total Registros", len(df_sonido))
            
            st.markdown("---")
            st.markdown("### 1.1 Evolución del Nivel de Sonido Promedio (dB)")
            
            columna_sensor = 'tenant_name' if 'tenant_name' in df_sonido.columns else 'device_name'
            
            if columna_sensor in df_sonido.columns and df_sonido[columna_sensor].notna().any():
                fig_evolucion = crear_grafico_evolucion_temporal(
                    df_sonido,
                    'LAeq',
                    "Evolución del Nivel de Sonido Promedio (dB)",
                    color_by=columna_sensor
                )
            else:
                fig_evolucion = crear_grafico_evolucion_temporal(
                    df_sonido,
                    'LAeq',
                    "Evolución del Nivel de Sonido Promedio (dB)"
                )
            st.plotly_chart(fig_evolucion, use_container_width=True)
            
            st.markdown("---")
            st.markdown("### 1.2 Nivel de Sonido Promedio por Sensor")
            
            if columna_sensor in df_sonido.columns and df_sonido[columna_sensor].notna().any():
                fig_barras = crear_grafico_barras_promedio(
                    df_sonido,
                    columna_sensor,
                    'LAeq',
                    "Nivel de Sonido Promedio (LAeq) por Sensor"
                )
                st.plotly_chart(fig_barras, use_container_width=True)
            else:
                st.info("No hay información de sensor disponible")
            
            st.markdown("---")
            st.markdown("### 1.3 Distribución del Nivel de Sonido por Sensor")
            
            if columna_sensor in df_sonido.columns and df_sonido[columna_sensor].notna().any():
                fig_box = crear_boxplot_distribucion(
                    df_sonido,
                    columna_sensor,
                    'LAeq',
                    "Distribución del Nivel de Sonido por Sensor"
                )
                st.plotly_chart(fig_box, use_container_width=True)
            else:
                st.info("No hay información de sensor disponible")
            
            st.markdown("---")
            st.markdown("### 1.4 Patrón de Ruido: Hora del Día vs. Día de la Semana")
            
            if len(df_sonido) > 0:
                fig_heatmap = crear_heatmap_hora_dia_semana(df_sonido, 'LAeq')
                if fig_heatmap is not None:
                    st.plotly_chart(fig_heatmap, use_container_width=True)
                else:
                    st.info("No hay suficientes datos para el heatmap")
            else:
                st.info("No hay suficientes datos para el heatmap")

# =============================
# CALIDAD DEL AIRE (EM500)
# =============================
elif menu == "🌫️ Calidad del Aire (EM500)":
    st.markdown("## 🌫️ Calidad del Aire - EM500")
    
    if df.empty:
        st.warning("⚠️ No hay datos disponibles para sensores de calidad del aire.")
    else:
        columnas_numericas = ['co2', 'temperature', 'humidity', 'pressure']
        for col in columnas_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            co2_prom = df['co2'].mean() if 'co2' in df.columns and df['co2'].notna().any() else 0
            st.metric("CO₂ Promedio (ppm)", f"{co2_prom:.1f}")
        with col2:
            temp_prom = df['temperature'].mean() if 'temperature' in df.columns and df['temperature'].notna().any() else 0
            st.metric("Temperatura Promedio (°C)", f"{temp_prom:.1f}")
        with col3:
            hum_prom = df['humidity'].mean() if 'humidity' in df.columns and df['humidity'].notna().any() else 0
            st.metric("Humedad Promedio (%)", f"{hum_prom:.1f}")
        with col4:
            pres_prom = df['pressure'].mean() if 'pressure' in df.columns and df['pressure'].notna().any() else 0
            st.metric("Presión Promedio (hPa)", f"{pres_prom:.1f}")
        
        st.markdown("---")
        
        if 'co2' in df.columns and df['co2'].notna().any():
            st.markdown("### 📈 Evolución Temporal de CO₂")
            if 'device_name' in df.columns:
                fig_co2 = crear_grafico_evolucion_temporal(
                    df,
                    'co2',
                    "CO₂ (ppm)",
                    color_by='device_name'
                )
            else:
                fig_co2 = crear_grafico_evolucion_temporal(df, 'co2', "CO₂ (ppm)")
            st.plotly_chart(fig_co2, use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            if 'temperature' in df.columns and df['temperature'].notna().any():
                st.markdown("### 🌡️ Temperatura")
                fig_temp = crear_grafico_evolucion_temporal(df, 'temperature', "Temperatura (°C)")
                st.plotly_chart(fig_temp, use_container_width=True)
        with col2:
            if 'humidity' in df.columns and df['humidity'].notna().any():
                st.markdown("### 💧 Humedad")
                fig_hum = crear_grafico_evolucion_temporal(df, 'humidity', "Humedad (%)")
                st.plotly_chart(fig_hum, use_container_width=True)
        
        if 'pressure' in df.columns and df['pressure'].notna().any():
            st.markdown("### 📊 Presión")
            fig_pres = crear_grafico_evolucion_temporal(df, 'pressure', "Presión (hPa)")
            st.plotly_chart(fig_pres, use_container_width=True)

# =============================
# SENSORES SOTERRADOS (EM310)
# =============================
elif menu == "🌱 Sensores Soterrados (EM310)":
    st.markdown("## 🌱 Sensores Soterrados - EM310")
    
    if df.empty:
        st.warning("⚠️ No hay datos disponibles para sensores soterrados.")
    else:
        if 'distance' in df.columns:
            df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
        
        col1, col2, col3 = st.columns(3)
        with col1:
            dist_prom = df['distance'].mean() if 'distance' in df.columns and df['distance'].notna().any() else 0
            st.metric("Distancia Promedio (cm)", f"{dist_prom:.1f}")
        with col2:
            dist_max = df['distance'].max() if 'distance' in df.columns and df['distance'].notna().any() else 0
            st.metric("Distancia Máxima (cm)", f"{dist_max:.1f}")
        with col3:
            st.metric("Total Registros", len(df))
        
        st.markdown("---")
        
        if 'distance' in df.columns and df['distance'].notna().any():
            st.markdown("### 📊 Evolución de Distancia")
            if 'device_name' in df.columns:
                fig_dist = crear_grafico_evolucion_temporal(
                    df,
                    'distance',
                    "Distancia (cm)",
                    color_by='device_name'
                )
            else:
                fig_dist = crear_grafico_evolucion_temporal(df, 'distance', "Distancia (cm)")
            st.plotly_chart(fig_dist, use_container_width=True)
        
        if 'status' in df.columns:
            st.markdown("### 📍 Estado de los Sensores")
            fig_status = px.pie(
                df,
                names='status',
                title="Distribución de Estados",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_status, use_container_width=True)
        
        if 'device_name' in df.columns and 'distance' in df.columns:
            st.markdown("### 📊 Distribución de Distancia por Sensor")
            fig_box_dist = crear_boxplot_distribucion(
                df,
                'device_name',
                'distance',
                "Distribución de Distancia por Sensor"
            )
            st.plotly_chart(fig_box_dist, use_container_width=True)

# =============================
# FOOTER
# =============================
st.markdown("---")
st.markdown("### 📝 Información del Sistema")
col1, col2, col3 = st.columns(3)
with col1:
    st.info(f"**Base de Datos:** {DB_NAME}")
with col2:
    st.info(f"**Última actualización:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
with col3:
    if 'df' in locals() and not df.empty:
        st.info(f"**Registros mostrados:** {len(df)}")
