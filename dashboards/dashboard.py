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
import json
from sklearn.metrics import confusion_matrix, accuracy_score, precision_score, recall_score, f1_score
import bcrypt
from supabase import create_client, Client

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

        # Validar que esté activo
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
        
        st.success("Inicio de sesión exitoso ✔")
        st.rerun()

if not st.session_state["is_authenticated"]:
    mostrar_login()
    st.stop()

# Botón para cerrar sesión (Sidebar)
def boton_logout():
    """Botón para cerrar sesión, en el sidebar."""
    if st.sidebar.button("🚪 Cerrar sesión"):
        uid = st.session_state.get("user_id")
        if uid:
            log_action(uid, "logout", "info", {})
        st.session_state.clear()
        st.success("Sesión cerrada.")
        st.rerun()

# =============================
# CONFIGURACIÓN DE BASES DE DATOS
# =============================
# MySQL
DB_HOST = "localhost"
DB_PORT = 3307  # Puerto mapeado en Docker
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"

# MongoDB Atlas
MONGO_ATLAS_URI = "mongodb+srv://jg012119:cEfOpibMb2iFfrCs@cluster0.oyerk.mongodb.net/emergentETLVALENTINA?retryWrites=true&w=majority&appName=Cluster0"
MONGO_COLLECTION = "sensores"

# =============================
# FUNCIONES DE CONEXIÓN
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
        
        # Verificar que la conexión esté viva
        if not conn.is_connected():
            logger.error("❌ La conexión a MySQL no está activa")
            st.error("❌ La conexión a MySQL no está activa. Intenta recargar.")
            return pd.DataFrame()
        
        logger.info(f"✅ Conexión establecida, ejecutando query...")
        query = f"SELECT * FROM `{tabla}` ORDER BY time DESC LIMIT 10000"
        
        # Usar pandas.read_sql directamente (más confiable)
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
        # Siempre cerrar la conexión en el finally
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
# FUNCIONES ML - PREDICCIONES
# =============================
def load_ml_predictions_regression(sensor_type, metric_name, date_from, date_to):
    """
    Carga predicciones de REGRESIÓN por rango de fechas.
    Usa la tabla ml_predictions_regression.
    """
    conn = None
    try:
        conn = get_mysql_connection()
        if conn is None:
            return pd.DataFrame()
        
        query = """
            SELECT sensor_type, device_name, time, metric_name, 
                   real_value, predicted_value, model_version
            FROM ml_predictions_regression
            WHERE sensor_type = %s 
              AND metric_name = %s
              AND time BETWEEN %s AND %s
            ORDER BY time
        """
        
        df = pd.read_sql(
            query,
            conn,
            params=(sensor_type, metric_name, date_from, date_to)
        )
        
        if not df.empty and 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
        
        return df
    except Exception as e:
        logger.error(f"❌ Error cargando predicciones de regresión: {e}")
        return pd.DataFrame()
    finally:
        if conn and conn.is_connected():
            conn.close()


def load_ml_predictions_classification(sensor_type, date_from, date_to):
    """
    Carga predicciones de CLASIFICACIÓN por rango de fechas.
    Usa la tabla ml_predictions_classification.
    """
    conn = None
    try:
        conn = get_mysql_connection()
        if conn is None:
            return pd.DataFrame()
        
        query = """
            SELECT sensor_type, device_name, time, 
                   real_class, predicted_class, confidence, model_version
            FROM ml_predictions_classification
            WHERE sensor_type = %s 
              AND time BETWEEN %s AND %s
            ORDER BY time
        """
        
        df = pd.read_sql(
            query,
            conn,
            params=(sensor_type, date_from, date_to)
        )
        
        if not df.empty and 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
        
        return df
    except Exception as e:
        logger.error(f"❌ Error cargando predicciones de clasificación: {e}")
        return pd.DataFrame()
    finally:
        if conn and conn.is_connected():
            conn.close()

def load_ml_metrics_regression(sensor_type, metric_name):
    """Carga métricas de regresión más recientes"""
    conn = None
    try:
        conn = get_mysql_connection()
        if conn is None:
            return None
        
        query = """
            SELECT r2_score, rmse, mae, sample_count, date_from, date_to
            FROM ml_metrics_regression
            WHERE sensor_type = %s AND metric_name = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, (sensor_type, metric_name))
        result = cursor.fetchone()
        cursor.close()
        
        return result
    except Exception as e:
        logger.error(f"Error cargando métricas de regresión: {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()

def load_ml_metrics_classification(sensor_type):
    """Carga métricas de clasificación más recientes"""
    conn = None
    try:
        conn = get_mysql_connection()
        if conn is None:
            return None
        
        query = """
            SELECT accuracy, precision_score, recall_score, f1_score, 
                   confusion_matrix_json, sample_count, date_from, date_to
            FROM ml_metrics_classification
            WHERE sensor_type = %s
            ORDER BY created_at DESC
            LIMIT 1
        """
        
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, (sensor_type,))
        result = cursor.fetchone()
        cursor.close()
        
        if result and result.get('confusion_matrix_json'):
            result['confusion_matrix_json'] = json.loads(result['confusion_matrix_json'])
        
        return result
    except Exception as e:
        logger.error(f"Error cargando métricas de clasificación: {e}")
        return None
    finally:
        if conn and conn.is_connected():
            conn.close()

def calculate_metrics_for_range(df_predictions, metric_name):
    """Calcula métricas (R², RMSE, MAE) para un rango específico de predicciones"""
    # Filtrar solo registros con valores reales (no futuros)
    df_with_real = df_predictions[df_predictions['real_value'].notna()].copy()
    
    if len(df_with_real) < 2:
        return None
    
    y_true = df_with_real['real_value'].values
    y_pred = df_with_real['predicted_value'].values
    
    # R²
    ss_res = np.sum((y_true - y_pred) ** 2)
    ss_tot = np.sum((y_true - np.mean(y_true)) ** 2)
    r2 = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
    
    # RMSE
    rmse = np.sqrt(np.mean((y_true - y_pred) ** 2))
    
    # MAE
    mae = np.mean(np.abs(y_true - y_pred))
    
    return {
        'r2': r2,
        'rmse': rmse,
        'mae': mae,
        'sample_count': len(df_with_real)
    }

def calculate_classification_metrics_for_range(df_predictions):
    """Calcula métricas de clasificación para un rango específico"""
    # Filtrar solo registros con clases reales (no futuros)
    df_with_real = df_predictions[df_predictions['real_class'].notna()].copy()
    
    if len(df_with_real) < 2:
        return None
    
    y_true = df_with_real['real_class'].values
    y_pred = df_with_real['predicted_class'].values
    
    # Calcular métricas
    accuracy = accuracy_score(y_true, y_pred)
    precision = precision_score(y_true, y_pred, average='weighted', zero_division=0)
    recall = recall_score(y_true, y_pred, average='weighted', zero_division=0)
    f1 = f1_score(y_true, y_pred, average='weighted', zero_division=0)
    
    # Matriz de confusión
    classes = ['Normal', 'Alerta', 'Crítico']
    cm = confusion_matrix(y_true, y_pred, labels=classes)
    
    return {
        'accuracy': accuracy,
        'precision': precision,
        'recall': recall,
        'f1': f1,
        'confusion_matrix': cm,
        'labels': classes,
        'sample_count': len(df_with_real)
    }

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
    # Crear copia para no modificar el original
    df_heat = df.copy()
    
    # Extraer día de la semana y hora
    df_heat['dia_semana'] = df_heat['time'].dt.day_name()
    df_heat['hora'] = df_heat['time'].dt.hour
    
    # Mapear días de la semana a números (0=Lunes, 6=Domingo)
    dias_orden = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dias_espanol = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
    df_heat['dia_num'] = df_heat['dia_semana'].map({dia: i for i, dia in enumerate(dias_orden)})
    
    # Filtrar valores válidos
    df_heat = df_heat[df_heat[columna_valor].notna() & df_heat['dia_num'].notna()]
    
    if df_heat.empty:
        return None
    
    # Calcular promedio por hora y día
    heatmap_data = df_heat.groupby(['dia_num', 'hora'])[columna_valor].mean().reset_index()
    
    # Crear pivot table
    heatmap_pivot = heatmap_data.pivot(index='hora', columns='dia_num', values=columna_valor)
    
    # Asegurar que todas las horas (0-23) y días (0-6) estén presentes
    horas_completas = pd.DataFrame({'hora': range(24)})
    dias_completos = list(range(7))
    
    # Reindexar para incluir todas las horas
    heatmap_pivot = heatmap_pivot.reindex(range(24))
    
    # Asegurar que todas las columnas de días estén presentes
    for dia in dias_completos:
        if dia not in heatmap_pivot.columns:
            heatmap_pivot[dia] = np.nan
    
    # Reordenar columnas según días de la semana
    heatmap_pivot = heatmap_pivot.reindex(columns=dias_completos)
    heatmap_pivot.columns = dias_espanol
    
    # Crear el heatmap
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
# FUNCIONES DE VISUALIZACIÓN ML
# =============================
def plot_real_vs_predicted(df_predictions, metric_name, rmse=None):
    """Gráfica de Real vs Predicho con Intervalo de Confianza"""
    fig = go.Figure()
    
    # Filtrar datos
    df_real = df_predictions[df_predictions['real_value'].notna()].copy()
    df_future = df_predictions[df_predictions['real_value'].isna()].copy()
    
    # --- Intervalo de Confianza (Si hay RMSE) ---
    if rmse and not df_future.empty:
        # Calcular límites solo para predicciones futuras
        upper_bound = df_future['predicted_value'] + (1.96 * rmse)
        lower_bound = df_future['predicted_value'] - (1.96 * rmse)
        
        # Área sombreada (Confidence Interval)
        fig.add_trace(go.Scatter(
            x=pd.concat([df_future['time'], df_future['time'][::-1]]),
            y=pd.concat([upper_bound, lower_bound[::-1]]),
            fill='toself',
            fillcolor='rgba(255, 165, 0, 0.2)',
            line=dict(color='rgba(255,255,255,0)'),
            hoverinfo="skip",
            name='Intervalo de Confianza (95%)'
        ))

    # --- Líneas de Datos ---
    # Real
    if not df_real.empty:
        fig.add_trace(go.Scatter(
            x=df_real['time'],
            y=df_real['real_value'],
            mode='lines+markers',
            name='Valores Reales',
            line=dict(color='#1f77b4', width=3),
            marker=dict(size=6)
        ))
        
        # Predicción sobre datos reales
        fig.add_trace(go.Scatter(
            x=df_real['time'], 
            y=df_real['predicted_value'],
            mode='lines',
            name='Ajuste del Modelo',
            line=dict(color='#ff7f0e', width=2, dash='dot'),
            opacity=0.7
        ))
    
    # Futuro
    if not df_future.empty:
        fig.add_trace(go.Scatter(
            x=df_future['time'],
            y=df_future['predicted_value'],
            mode='lines+markers',
            name='Pronóstico Futuro',
            line=dict(color='#ff7f0e', width=3),
            marker=dict(size=8, symbol='star')
        ))
    
    fig.update_layout(
        title=f"Pronóstico y Fiabilidad - {metric_name}",
        xaxis_title="Fecha",
        yaxis_title=metric_name,
        hovermode='x unified',
        height=550,
        legend=dict(orientation="h", y=1.1)
    )
    
    return fig

def plot_classification_timeline(df_class, labels):
    """Timeline de clasificación con nivel de confianza"""
    if df_class.empty:
        return None
        
    # Mapear clases a colores
    color_map = {
        "Normal": "#2ca02c",  # Verde
        "Alerta": "#ff7f0e",  # Naranja
        "Crítico": "#d62728", # Rojo
        "Desconocido": "#7f7f7f"
    }
    
    fig = px.scatter(
        df_class,
        x='time',
        y='predicted_class',
        color='predicted_class',
        size='confidence',
        color_discrete_map=color_map,
        labels={'confidence': 'Nivel de Confianza', 'predicted_class': 'Estado'},
        title="Línea de Tiempo de Estados y Confiabilidad",
        category_orders={"predicted_class": ["Normal", "Alerta", "Crítico"]}
    )
    
    fig.update_traces(marker=dict(line=dict(width=1, color='DarkSlateGrey')))
    
    fig.update_layout(
        height=400,
        yaxis_title="Estado Predicho",
        xaxis_title="Tiempo",
        showlegend=True
    )
    
    return fig

def plot_error_distribution(df_predictions, metric_name):
    """Gráfica de distribución de errores (RF-01)"""
    # Solo para datos con valores reales
    df_real = df_predictions[df_predictions['real_value'].notna()].copy()
    
    if df_real.empty or len(df_real) < 2:
        return None
    
    # Calcular error absoluto
    df_real['error'] = np.abs(df_real['real_value'] - df_real['predicted_value'])
    
    fig = go.Figure()
    
    # Gráfica de barras de error
    fig.add_trace(go.Bar(
        x=df_real['time'],
        y=df_real['error'],
        name='Error Absoluto',
        marker=dict(
            color=df_real['error'],
            colorscale='Reds',
            showscale=True,
            colorbar=dict(title="Error")
        )
    ))
    
    fig.update_layout(
        title=f"Error Absoluto (|Real - Predicho|) - {metric_name}",
        xaxis_title="Fecha",
        yaxis_title="Error Absoluto",
        height=400,
        showlegend=False
    )
    
    return fig

def plot_confusion_matrix(confusion_matrix_data, labels):
    """Gráfica de matriz de confusión (RF-02)"""
    if confusion_matrix_data is None:
        return None
    
    # Si es un array numpy, convertir a lista
    if isinstance(confusion_matrix_data, np.ndarray):
        cm = confusion_matrix_data
    else:
        cm = np.array(confusion_matrix_data)
    
    # Crear anotaciones con porcentajes
    annotations = []
    for i in range(len(labels)):
        for j in range(len(labels)):
            value = cm[i, j]
            total = cm[i].sum()
            percentage = (value / total * 100) if total > 0 else 0
            
            annotations.append(
                dict(
                    x=j,
                    y=i,
                    text=f"{int(value)}<br>({percentage:.1f}%)",
                    showarrow=False,
                    font=dict(color="white" if value > cm.max()/2 else "black", size=12)
                )
            )
    
    fig = go.Figure(data=go.Heatmap(
        z=cm,
        x=[f"Pred: {label}" for label in labels],
        y=[f"Real: {label}" for label in labels],
        colorscale='Blues',
        showscale=True,
        colorbar=dict(title="Frecuencia")
    ))
    
    fig.update_layout(
        title="Matriz de Confusión",
        xaxis_title="Clase Predicha",
        yaxis_title="Clase Real",
        height=500,
        annotations=annotations
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

# Definir opciones del menú basado en el rol
menu_options = [
    "🔊 Calidad del Sonido (WS302)",
    "🌫️ Calidad del Aire (EM500)",
    "🌱 Sensores Soterrados (EM310)",
    "🤖 Machine Learning"
]

# Si es ejecutivo, solo mostrar Machine Learning
# Convertimos a minúsculas para asegurar coincidencia
if user_role and user_role.lower() == "ejecutivo":
    menu_options = ["🤖 Machine Learning"]

menu = st.sidebar.radio(
    "Selecciona una sección",
    menu_options
)

st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Configuración")
auto_refresh = st.sidebar.checkbox("Auto-refrescar", value=False)
if auto_refresh:
    refresh_interval = st.sidebar.slider("Intervalo (segundos)", 5, 60, 10)
    st.sidebar.info(f"🔄 Actualizando cada {refresh_interval}s")

# Botón para limpiar cache
if st.sidebar.button("🔄 Limpiar Cache y Recargar"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.success("✅ Cache limpiado")
    st.rerun()

# Opción para deshabilitar cache
sin_cache = st.sidebar.checkbox("🚫 Deshabilitar cache (más lento pero siempre actualizado)", value=False)

# =============================
# CARGA DE DATOS
# =============================
st.title("📊 Dashboard Ambiental - GAMC")
st.markdown("---")

# Mostrar indicador de carga
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
        
        # Debug: mostrar información en el sidebar
        if st.sidebar.checkbox("🔍 Mostrar información de debug", value=False):
            st.sidebar.write(f"**Tabla consultada:** {menu}")
            st.sidebar.write(f"**Registros encontrados:** {len(df)}")
            if not df.empty:
                st.sidebar.write(f"**Columnas:** {list(df.columns)}")
                st.sidebar.write(f"**Primera fecha:** {df['time'].min() if 'time' in df.columns else 'N/A'}")
                st.sidebar.write(f"**Última fecha:** {df['time'].max() if 'time' in df.columns else 'N/A'}")
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
        # Convertir columnas numéricas
        if 'LAeq' in df.columns:
            df['LAeq'] = pd.to_numeric(df['LAeq'], errors='coerce')
        if 'LAI' in df.columns:
            df['LAI'] = pd.to_numeric(df['LAI'], errors='coerce')
        if 'LAImax' in df.columns:
            df['LAImax'] = pd.to_numeric(df['LAImax'], errors='coerce')
        
        # Filtrar datos válidos
        df_sonido = df[df['LAeq'].notna()].copy()
        
        if df_sonido.empty:
            st.warning("⚠️ No hay datos válidos de LAeq.")
        else:
            # Métricas principales
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
            
            # 1.1 Evolución del Nivel de Sonido Promedio (dB)
            st.markdown("### 1.1 Evolución del Nivel de Sonido Promedio (dB)")
            
            # Usar tenant_name si existe, sino device_name
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
            
            # 1.2 Nivel de Sonido Promedio por Sensor
            st.markdown("### 1.2 Nivel de Sonido Promedio por Sensor")
            
            # Usar tenant_name si existe, sino device_name
            columna_sensor = 'tenant_name' if 'tenant_name' in df_sonido.columns else 'device_name'
            
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
            
            # 1.3 Distribución del Nivel de Sonido por Sensor
            st.markdown("### 1.3 Distribución del Nivel de Sonido por Sensor")
            
            # Usar tenant_name si existe, sino device_name
            columna_sensor = 'tenant_name' if 'tenant_name' in df_sonido.columns else 'device_name'
            
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
            
            # 1.4 Patrón de Ruido: Hora del Día vs. Día de la Semana
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
        # Convertir columnas numéricas
        columnas_numericas = ['co2', 'temperature', 'humidity', 'pressure']
        for col in columnas_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Métricas principales
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
        
        # Evolución temporal de CO2
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
        
        # Gráficos en columnas
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
        
        # Presión
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
        # Convertir columnas numéricas
        if 'distance' in df.columns:
            df['distance'] = pd.to_numeric(df['distance'], errors='coerce')
        
        # Métricas principales
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
        
        # Evolución temporal de distancia
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
        
        # Estado de los sensores
        if 'status' in df.columns:
            st.markdown("### 📍 Estado de los Sensores")
            fig_status = px.pie(
                df,
                names='status',
                title="Distribución de Estados",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig_status, use_container_width=True)
        
        # Distribución por sensor
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
# MACHINE LEARNING (RF-01 & RF-02)
# =============================
elif menu == "🤖 Machine Learning":
    st.markdown("## 🤖 Machine Learning - Predicciones y Análisis")
    st.markdown("*Análisis de modelos predictivos con visualización dinámica por rangos de fechas*")
    
    # Tabs para Regresión y Clasificación
    ml_tab1, ml_tab2 = st.tabs(["📈 Regresión (RF-01)", "📊 Clasificación (RF-02)"])
    
    # =============================
    # TAB 1: REGRESIÓN (RF-01)
    # =============================
    with ml_tab1:
        st.markdown("### RF-01: Generación Dinámica de Gráficas por Rango de Fechas")
        st.markdown("Selecciona un rango de fechas para visualizar predicciones vs valores reales")
        
        # Selector de rango de fechas
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            date_from = st.date_input(
                "Fecha Inicio",
                value=datetime.now() - timedelta(days=7),
                max_value=datetime.now() + timedelta(days=7)
            )
        with col_date2:
            date_to = st.date_input(
                "Fecha Fin",
                value=datetime.now() + timedelta(days=3),
                max_value=datetime.now() + timedelta(days=30)
            )
        
        # Selector de tipo de sensor
        sensor_type_map = {
            "EM500 - Calidad del Aire (CO2)": ("em500_co2", ["co2", "temperature", "humidity", "pressure"]),
            "WS302 - Calidad del Sonido": ("ws302_sonido", ["LAeq", "LAI", "LAImax"]),
            "EM310 - Sensores Soterrados": ("em310_soterrados", ["distance"])
        }
        
        sensor_display = st.selectbox(
            "Selecciona tipo de sensor",
            list(sensor_type_map.keys())
        )
        
        sensor_type, available_metrics = sensor_type_map[sensor_display]
        
        metric_name = st.selectbox(
            "Selecciona métrica",
            available_metrics
        )
        
        if st.button("🔮 Cargar Predicciones", type="primary"):
            with st.spinner("Cargando predicciones..."):
                # Cargar predicciones
                df_predictions = load_ml_predictions_regression(
                    sensor_type,
                    metric_name,
                    date_from,
                    date_to
                )
                
                if df_predictions.empty:
                    # No hay predicciones - Generar automáticamente
                    st.info("🔮 No hay predicciones para este rango. Generando predicciones automáticamente...")
                    
                    # Mostrar progreso
                    with st.spinner('⏳ Generando predicciones realistas... Esto puede tomar 20-30 segundos...'):
                        import subprocess
                        
                        try:
                            # Ejecutar el predictor
                            result = subprocess.run(
                                [
                                    'docker', 'exec', 'spark-master', 
                                    'python3', '/opt/spark/app/ml/quick_predictor.py',
                                    date_from.strftime('%Y-%m-%d'),
                                    date_to.strftime('%Y-%m-%d')
                                ],
                                capture_output=True,
                                text=True,
                                timeout=120
                            )
                            
                            if result.returncode == 0:
                                st.success("✅ Predicciones generadas exitosamente!")
                                st.info("🔄 Cargando predicciones generadas...")
                                
                                # Recargar predicciones
                                df_predictions = load_ml_predictions_regression(
                                    sensor_type,
                                    metric_name,
                                    date_from,
                                    date_to
                                )
                                
                                if df_predictions.empty:
                                    st.error("❌ Error: Las predicciones se generaron pero no se pudieron cargar. Intenta recargar la página.")
                                else:
                                    st.success(f"✅ {len(df_predictions)} predicciones cargadas")
                            else:
                                st.error(f"❌ Error generando predicciones: {result.stderr}")
                                st.code(result.stdout)
                                
                        except subprocess.TimeoutExpired:
                            st.error("⏱️ Timeout: La generación de predicciones está tomando más tiempo del esperado. Intenta con un rango de fechas más pequeño.")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")
                            st.info("💡 Puedes generar predicciones manualmente ejecutando: `generar_predicciones.bat`")
                
                # Mostrar resultados solo si hay predicciones
                if not df_predictions.empty:
                    st.success(f"✅ {len(df_predictions)} predicciones cargadas")
                    
                    # Determinar si hay datos reales o solo futuros
                    has_real_data = df_predictions['real_value'].notna().any()
                    future_only = df_predictions['real_value'].isna().all()
                    
                    # Información del rango
                    col_info1, col_info2, col_info3 = st.columns(3)
                    with col_info1:
                        real_count = df_predictions['real_value'].notna().sum()
                        st.metric("Datos Reales", real_count)
                    with col_info2:
                        pred_count = len(df_predictions)
                        st.metric("Total Predicciones", pred_count)
                    with col_info3:
                        future_count = df_predictions['real_value'].isna().sum()
                        st.metric("Predicciones Futuras", future_count)
                    
                    st.markdown("---")
                    
                    # =============================
                    # GRÁFICA REAL VS PREDICHO
                    # =============================
                    st.markdown("### 📊 Real vs Predicción")
                    
                    if future_only:
                        st.info("ℹ️ El rango seleccionado solo contiene fechas futuras. Mostrando únicamente predicciones.")
                    
                    # Obtener RMSE Global para intervalo de confianza
                    global_rmse = None
                    global_metrics = load_ml_metrics_regression(sensor_type, metric_name)
                    if global_metrics:
                        global_rmse = float(global_metrics['rmse'])

                    fig_real_pred = plot_real_vs_predicted(df_predictions, metric_name, rmse=global_rmse)
                    st.plotly_chart(fig_real_pred, use_container_width=True)
                    
                    st.markdown("---")
                    
                    # =============================
                    # MÉTRICAS DINÁMICAS (R², RMSE, MAE)
                    # =============================
                    if has_real_data and not future_only:
                        st.markdown("### 📈 Métricas del Modelo (Rango Seleccionado)")
                        
                        # Calcular métricas para el rango
                        range_metrics = calculate_metrics_for_range(df_predictions, metric_name)
                        
                        if range_metrics:
                            col_m1, col_m2, col_m3, col_m4 = st.columns(4)
                            
                            with col_m1:
                                r2_val = range_metrics['r2']
                                r2_color = "normal" if r2_val > 0.5 else "inverse"
                                st.metric(
                                    "R² (Coef. Determinación)",
                                    f"{r2_val:.4f}",
                                    delta="Bueno" if r2_val > 0.7 else "Regular" if r2_val > 0.5 else "Bajo",
                                    delta_color=r2_color
                                )
                            
                            with col_m2:
                                st.metric("RMSE", f"{range_metrics['rmse']:.4f}")
                            
                            with col_m3:
                                st.metric("MAE", f"{range_metrics['mae']:.4f}")
                            
                            with col_m4:
                                st.metric("Muestras", range_metrics['sample_count'])
                            
                            # Comparar con métricas globales del modelo
                            st.markdown("#### Comparación con Métricas Globales")
                            
                            global_metrics = load_ml_metrics_regression(sensor_type, metric_name)
                            
                            if global_metrics:
                                comp_col1, comp_col2, comp_col3 = st.columns(3)
                                
                                with comp_col1:
                                    st.write("**R² Global:**", f"{float(global_metrics['r2_score']):.4f}")
                                    st.write("**R² Rango:**", f"{float(range_metrics['r2']):.4f}")
                                    diff_r2 = float(range_metrics['r2']) - float(global_metrics['r2_score'])
                                    st.write("**Diferencia:**", f"{diff_r2:+.4f}")
                                
                                with comp_col2:
                                    st.write("**RMSE Global:**", f"{float(global_metrics['rmse']):.4f}")
                                    st.write("**RMSE Rango:**", f"{float(range_metrics['rmse']):.4f}")
                                
                                with comp_col3:
                                    st.write("**MAE Global:**", f"{float(global_metrics['mae']):.4f}")
                                    st.write("**MAE Rango:**", f"{float(range_metrics['mae']):.4f}")
                        
                        st.markdown("---")
                        
                        # =============================
                        # GRÁFICA DE ERROR
                        # =============================
                        st.markdown("### 📉 Distribución de Error")
                        
                        fig_error = plot_error_distribution(df_predictions, metric_name)
                        
                        if fig_error:
                            st.plotly_chart(fig_error, use_container_width=True)
                            
                            # Estadísticas de error
                            df_real = df_predictions[df_predictions['real_value'].notna()].copy()
                            df_real['error'] = np.abs(df_real['real_value'] - df_real['predicted_value'])
                            
                            err_col1, err_col2, err_col3 = st.columns(3)
                            with err_col1:
                                st.metric("Error Promedio", f"{df_real['error'].mean():.4f}")
                            with err_col2:
                                st.metric("Error Máximo", f"{df_real['error'].max():.4f}")
                            with err_col3:
                                st.metric("Error Mínimo", f"{df_real['error'].min():.4f}")
                        else:
                            st.info("No hay suficientes datos para mostrar la distribución de error")
                    else:
                        st.info("ℹ️ Las métricas y gráficas de error solo se muestran cuando el rango incluye datos reales.")
    
    # =============================
    # TAB 2: CLASIFICACIÓN (RF-02)
    # =============================
    with ml_tab2:
        st.markdown("### RF-02: Matriz de Confusión (Clasificación)")
        st.markdown("Visualización de matriz de confusión para modelos de clasificación")
        st.markdown("*Aplica solo a **EM310 - Sensores Soterrados** (clasificación de nivel de alerta)*")
        
        # Selector de rango de fechas
        col_date1, col_date2 = st.columns(2)
        with col_date1:
            class_date_from = st.date_input(
                "Fecha Inicio (Clasificación)",
                value=datetime.now() - timedelta(days=7),
                max_value=datetime.now() + timedelta(days=7),
                key="class_date_from"
            )
        with col_date2:
            class_date_to = st.date_input(
                "Fecha Fin (Clasificación)",
                value=datetime.now(),
                max_value=datetime.now() + timedelta(days=30),
                key="class_date_to"
            )
        
        if st.button("📊 Cargar Clasificaciones", type="primary", key="load_classification"):
            with st.spinner("Cargando clasificaciones..."):
                # Cargar predicciones de clasificación
                df_class = load_ml_predictions_classification(
                    "em310_soterrados",
                    class_date_from,
                    class_date_to
                )
                
                if df_class.empty:
                    st.warning("⚠️ No hay clasificaciones disponibles para este rango. Verifica que el modelo esté entrenado.")
                else:
                    # Verificar si hay datos reales
                    has_real_classes = df_class['real_class'].notna().any()
                    future_only_class = df_class['real_class'].isna().all()
                    
                    if future_only_class:
                        st.info("ℹ️ El rango seleccionado solo contiene fechas futuras. La matriz de confusión solo se muestra con datos reales.")
                        
                        # Mostrar predicciones futuras
                        st.markdown("### 🔮 Predicciones Futuras")
                        
                        # Distribución de predicciones
                        pred_dist = df_class['predicted_class'].value_counts()
                        
                        fig_pred = px.pie(
                            values=pred_dist.values,
                            names=pred_dist.index,
                            title="Distribución de Predicciones Futuras",
                            color_discrete_sequence=px.colors.qualitative.Set3
                        )
                        st.plotly_chart(fig_pred, use_container_width=True)

                        # --- NUEVO: Timeline de Confianza ---
                        st.markdown("#### ⏳ Evolución de Estados y Confianza")
                        fig_timeline = plot_classification_timeline(df_class, ["Normal", "Alerta", "Crítico"])
                        if fig_timeline:
                            st.plotly_chart(fig_timeline, use_container_width=True)
                        
                        # Tabla de predicciones
                        st.markdown("#### Tabla de Predicciones")
                        st.dataframe(
                            df_class[['time', 'device_name', 'predicted_class', 'confidence']].head(20),
                            use_container_width=True
                        )
                    else:
                        st.success(f"✅ {len(df_class)} clasificaciones cargadas")
                        
                        # Métricas generales
                        real_count_class = df_class['real_class'].notna().sum()
                        future_count_class = df_class['real_class'].isna().sum()
                        
                        col_info1, col_info2, col_info3 = st.columns(3)
                        with col_info1:
                            st.metric("Total Clasificaciones", len(df_class))
                        with col_info2:
                            st.metric("Con Datos Reales", real_count_class)
                        with col_info3:
                            st.metric("Predicciones Futuras", future_count_class)
                        
                        st.markdown("---")
                        
                        # =============================
                        # MATRIZ DE CONFUSIÓN
                        # =============================
                        st.markdown("### 📊 Matriz de Confusión")
                        
                        # Calcular métricas del rango
                        class_metrics = calculate_classification_metrics_for_range(df_class)
                        
                        if class_metrics:
                            # Mostrar matriz de confusión
                            fig_cm = plot_confusion_matrix(
                                class_metrics['confusion_matrix'],
                                class_metrics['labels']
                            )
                            st.plotly_chart(fig_cm, use_container_width=True)
                            
                            st.markdown("---")
                            
                            # =============================
                            # MÉTRICAS DE CLASIFICACIÓN
                            # =============================
                            st.markdown("### 📈 Métricas de Clasificación (Rango Seleccionado)")
                            
                            met_col1, met_col2, met_col3, met_col4 = st.columns(4)
                            
                            with met_col1:
                                acc = class_metrics['accuracy']
                                st.metric(
                                    "Accuracy",
                                    f"{acc:.4f}",
                                    delta="Bueno" if acc > 0.8 else "Regular" if acc > 0.6 else "Bajo"
                                )
                            
                            with met_col2:
                                prec = class_metrics['precision']
                                st.metric("Precision", f"{prec:.4f}")
                            
                            with met_col3:
                                rec = class_metrics['recall']
                                st.metric("Recall", f"{rec:.4f}")
                            
                            with met_col4:
                                f1 = class_metrics['f1']
                                st.metric("F1-Score", f"{f1:.4f}")
                            
                            # Detalles de la matriz
                            st.markdown("#### Detalles de la Matriz")
                            
                            cm = class_metrics['confusion_matrix']
                            labels = class_metrics['labels']
                            
                            # Calcular VP, FP, VN, FN por clase
                            st.markdown("**Métricas por Clase:**")
                            
                            for i, label in enumerate(labels):
                                vp = cm[i, i]
                                fp = cm[:, i].sum() - vp
                                fn = cm[i, :].sum() - vp
                                vn = cm.sum() - vp - fp - fn
                                
                                with st.expander(f"Clase: {label}"):
                                    detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
                                    with detail_col1:
                                        st.metric("VP (Verdaderos Positivos)", int(vp))
                                    with detail_col2:
                                        st.metric("FP (Falsos Positivos)", int(fp))
                                    with detail_col3:
                                        st.metric("VN (Verdaderos Negativos)", int(vn))
                                    with detail_col4:
                                        st.metric("FN (Falsos Negativos)", int(fn))
                            
                            # Comparar con métricas globales
                            st.markdown("---")
                            st.markdown("#### Comparación con Métricas Globales")
                            
                            global_class_metrics = load_ml_metrics_classification("em310_soterrados")
                            
                            if global_class_metrics:
                                comp_col1, comp_col2, comp_col3, comp_col4 = st.columns(4)
                                
                                with comp_col1:
                                    st.write("**Accuracy Global:**", f"{global_class_metrics['accuracy']:.4f}")
                                    st.write("**Accuracy Rango:**", f"{class_metrics['accuracy']:.4f}")
                                
                                with comp_col2:
                                    st.write("**Precision Global:**", f"{global_class_metrics['precision_score']:.4f}")
                                    st.write("**Precision Rango:**", f"{class_metrics['precision']:.4f}")
                                
                                with comp_col3:
                                    st.write("**Recall Global:**", f"{global_class_metrics['recall_score']:.4f}")
                                    st.write("**Recall Rango:**", f"{class_metrics['recall']:.4f}")
                                
                                with comp_col4:
                                    st.write("**F1 Global:**", f"{global_class_metrics['f1_score']:.4f}")
                                    st.write("**F1 Rango:**", f"{class_metrics['f1']:.4f}")
                            
                            # Botón de descarga
                            st.markdown("---")
                            st.markdown("### 💾 Exportar Resultados")
                            
                            # Crear DataFrame de reporte
                            report_data = {
                                'Métrica': ['Accuracy', 'Precision', 'Recall', 'F1-Score', 'Muestras'],
                                'Valor': [
                                    f"{class_metrics['accuracy']:.4f}",
                                    f"{class_metrics['precision']:.4f}",
                                    f"{class_metrics['recall']:.4f}",
                                    f"{class_metrics['f1']:.4f}",
                                    class_metrics['sample_count']
                                ]
                            }
                            df_report = pd.DataFrame(report_data)
                            
                            csv = df_report.to_csv(index=False)
                            st.download_button(
                                label="📥 Descargar Reporte CSV",
                                data=csv,
                                file_name=f"classification_report_{class_date_from}_{class_date_to}.csv",
                                mime="text/csv"
                            )
                        else:
                            st.warning("No hay suficientes datos reales para calcular métricas de clasificación")

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
    if not df.empty:
        st.info(f"**Registros mostrados:** {len(df)}")
