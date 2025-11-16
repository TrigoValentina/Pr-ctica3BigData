import streamlit as st
from jwt_utils import verify_token
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
import plotly.graph_objects as go
from statsmodels.tsa.seasonal import seasonal_decompose
import json

# =============================
# CONFIGURACIÓN DE PÁGINA
# =============================
st.set_page_config(
    page_title="Dashboard Operador - GAMC",
    page_icon="📟",
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
@st.cache_data(ttl=10)  # Recarga cada 10 segundos
def leer_datos_kafka(broker, max_mensajes=200):
    """Lee los últimos mensajes de Kafka desde el topic configurado."""
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
            return df.sort_values('time', ascending=False)
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
role_requerido = "operador"   # CAMBIAR por operador / ejecutivo
if decoded.get("role") != role_requerido:
    st.error("Acceso denegado.")
    st.stop()

# =============================
# SIDEBAR Y LOGOUT
# =============================
st.sidebar.image("https://cdn-icons-png.flaticon.com/512/4149/4149670.png", width=110)
st.sidebar.title(f"Panel Operador")
st.sidebar.write(f"Bienvenido, **{decoded.get('username')}**.")
st.sidebar.markdown("---")

menu = st.sidebar.radio(
    "📊 Selecciona una sección",
    ["Calidad del Aire (EM500)", "Calidad del Sonido (WS302)", "Sensores Soterrados (EM310)"]
)

st.sidebar.markdown("---")
if st.sidebar.button("Cerrar sesión"):
    st.session_state.clear()
    st.switch_page("pages/auth_app.py")

# =============================
# CARGA DE DATOS
# =============================
df = leer_datos_kafka(KAFKA_BROKER)

if df.empty:
    st.warning("No se encontraron datos recientes en Kafka. Esperando nuevos mensajes...")
    st.stop()

# =============================
# PÁGINA: CALIDAD DEL AIRE
# =============================
if menu == "Calidad del Aire (EM500)":
    st.markdown("## 🌫️ Calidad del Aire - Sensor EM500")
    
    # Filtrar DF para solo tener datos de este sensor (si la columna 'object.co2' existe y no es nula)
    df_aire = df[df['object.co2'].notna()] if 'object.co2' in df.columns else pd.DataFrame()

    if not df_aire.empty:
        df_ts_aire = df_aire.set_index('time').sort_index()
        valor_actual_co2 = df_ts_aire['object.co2'].iloc[-1]

        # --- Layout: Métricas y Gauge ---
        col_m1, col_m2 = st.columns([2, 1])
        with col_m1:
            col1, col2, col3 = st.columns(3)
            col1.metric("CO₂ Promedio (ppm)", f"{df_aire['object.co2'].mean():.1f}")
            col2.metric("Temperatura (°C)", f"{df_aire['object.temperature'].mean():.1f}")
            col3.metric("Humedad (%)", f"{df_aire['object.humidity'].mean():.1f}")
        
        with col_m2:
            # Gráfico de Medidor (Gauge) para CO2
            fig_gauge_co2 = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = valor_actual_co2,
                title = {'text': "Nivel de CO₂ Actual (ppm)"},
                gauge = {'axis': {'range': [400, 2000]},
                         'bar': {'color': "#2196f3"},
                         'steps' : [
                             {'range': [400, 1000], 'color': "lightgreen"},
                             {'range': [1000, 1500], 'color': "yellow"},
                             {'range': [1500, 2000], 'color': "red"}],
                         }))
            fig_gauge_co2.update_layout(height=200, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_gauge_co2, use_container_width=True)

        st.markdown("### 📈 Evolución Temporal de Métricas Clave")
        
        # Gráfico CO₂ con Bollinger
        window_size = 7
        rolling_mean_co2 = df_ts_aire['object.co2'].rolling(window=window_size).mean()
        rolling_std_co2 = df_ts_aire['object.co2'].rolling(window=window_size).std()
        upper_band_co2 = rolling_mean_co2 + (rolling_std_co2 * 2)
        lower_band_co2 = rolling_mean_co2 - (rolling_std_co2 * 2)

        fig_co2_bollinger = go.Figure()
        fig_co2_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=df_ts_aire['object.co2'], mode='lines', name='CO₂', line=dict(color='#2196f3')))
        fig_co2_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=rolling_mean_co2, mode='lines', name=f'Media Móvil', line=dict(color='#e76f51', dash='dash')))
        fig_co2_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=upper_band_co2, mode='lines', name='Banda Superior', line=dict(width=0)))
        fig_co2_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=lower_band_co2, mode='lines', name='Banda Inferior', line=dict(width=0), fill='tonexty', fillcolor='rgba(33, 150, 243, 0.2)'))
        fig_co2_bollinger.update_layout(title="Evolución del CO₂ (ppm)", yaxis_title="CO₂ (ppm)")
        st.plotly_chart(fig_co2_bollinger, use_container_width=True)

        # Gráfico Temperatura con Bollinger
        rolling_mean_temp = df_ts_aire['object.temperature'].rolling(window=window_size).mean()
        rolling_std_temp = df_ts_aire['object.temperature'].rolling(window=window_size).std()
        upper_band_temp = rolling_mean_temp + (rolling_std_temp * 2)
        lower_band_temp = rolling_mean_temp - (rolling_std_temp * 2)

        fig_temp_bollinger = go.Figure()
        fig_temp_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=df_ts_aire['object.temperature'], mode='lines', name='Temperatura', line=dict(color='#e76f51')))
        fig_temp_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=rolling_mean_temp, mode='lines', name=f'Media Móvil', line=dict(color='#2a9d8f', dash='dash')))
        fig_temp_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=upper_band_temp, mode='lines', name='Banda Superior', line=dict(width=0)))
        fig_temp_bollinger.add_trace(go.Scatter(x=df_ts_aire.index, y=lower_band_temp, mode='lines', name='Banda Inferior', line=dict(width=0), fill='tonexty', fillcolor='rgba(231, 111, 81, 0.2)'))
        fig_temp_bollinger.update_layout(title="Evolución de la Temperatura (°C)", yaxis_title="Temperatura (°C)")
        st.plotly_chart(fig_temp_bollinger, use_container_width=True)

        st.markdown("### 🔗 Correlación entre Métricas")
        # Mapa de Calor de Correlaciones
        corr_cols = ['object.co2', 'object.temperature', 'object.humidity', 'object.pressure']
        # Renombrar columnas para que se vean mejor en el gráfico
        df_corr = df_aire[corr_cols].rename(columns={'object.co2': 'CO₂', 'object.temperature': 'Temperatura', 'object.humidity': 'Humedad', 'object.pressure': 'Presión'})
        corr_matrix = df_corr.corr()
        fig_corr = px.imshow(corr_matrix, text_auto=True, aspect="auto", title="Mapa de Calor de Correlaciones", color_continuous_scale='RdYlBu_r')
        st.plotly_chart(fig_corr, use_container_width=True)

    else:
        st.info("ℹ️ No se encontraron datos recientes para el sensor de Calidad del Aire.")

# =============================
# PÁGINA: CALIDAD DEL SONIDO
# =============================
elif menu == "Calidad del Sonido (WS302)":
    st.markdown("## 🔊 Calidad del Sonido - Sensor WS302")
    
    col_ruido = 'object.noise' if 'object.noise' in df.columns else 'object.laeq'
    # Filtrar DF para solo tener datos de este sensor
    df_sonido = df[df[col_ruido].notna()] if col_ruido in df.columns else pd.DataFrame()

    if not df_sonido.empty:
        df_ts_sonido = df_sonido.set_index('time').sort_index()
        valor_actual_ruido = df_ts_sonido[col_ruido].iloc[-1]

        col1, col2, col3 = st.columns(3)
        with col1:
            # Gráfico de Medidor (Gauge)
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = valor_actual_ruido,
                title = {'text': "Nivel de Ruido Actual (dB)"},
                gauge = {'axis': {'range': [30, 100]},
                         'bar': {'color': "#0077b6"},
                         'steps' : [
                             {'range': [30, 60], 'color': "lightgreen"},
                             {'range': [60, 80], 'color': "yellow"},
                             {'range': [80, 100], 'color': "red"}],
                         'threshold' : {'line': {'color': "black", 'width': 4}, 'thickness': 0.75, 'value': 90}}))
            fig_gauge.update_layout(height=250, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_gauge, use_container_width=True)

        col2.metric("Ruido Promedio (dB)", f"{df_sonido[col_ruido].mean():.1f}")
        col3.metric("Nivel Máximo (dB)", f"{df_sonido[col_ruido].max():.1f}")

        st.markdown("### 🔊 Evolución del Ruido con Bandas de Bollinger")
        window_size = 7
        rolling_mean_noise = df_ts_sonido[col_ruido].rolling(window=window_size).mean()
        rolling_std_noise = df_ts_sonido[col_ruido].rolling(window=window_size).std()
        upper_band_noise = rolling_mean_noise + (rolling_std_noise * 2)
        lower_band_noise = rolling_mean_noise - (rolling_std_noise * 2)

        fig_noise_bollinger = go.Figure()
        fig_noise_bollinger.add_trace(go.Scatter(x=df_ts_sonido.index, y=df_ts_sonido[col_ruido], mode='lines', name='Ruido', line=dict(color='#0077b6')))
        fig_noise_bollinger.add_trace(go.Scatter(x=df_ts_sonido.index, y=rolling_mean_noise, mode='lines', name='Media Móvil', line=dict(color='#e76f51', dash='dash')))
        fig_noise_bollinger.add_trace(go.Scatter(x=df_ts_sonido.index, y=upper_band_noise, mode='lines', name='Banda Superior', line=dict(width=0)))
        fig_noise_bollinger.add_trace(go.Scatter(x=df_ts_sonido.index, y=lower_band_noise, mode='lines', name='Banda Inferior', line=dict(width=0), fill='tonexty', fillcolor='rgba(0, 119, 182, 0.2)'))
        fig_noise_bollinger.update_layout(title="Evolución del Nivel de Ruido (dB)", yaxis_title="Ruido (dB)")
        st.plotly_chart(fig_noise_bollinger, use_container_width=True)

        st.markdown("---")
        st.markdown("### 📊 Análisis Adicional del Ruido")

        col_c, col_d = st.columns(2)

        with col_c:
            # Boxplot de Ruido por Hora
            st.markdown("#### Distribución del Ruido por Hora")
            df_sonido_box = df_sonido.copy()
            df_sonido_box['hora_dia'] = df_sonido_box['time'].dt.hour
            fig_boxplot_hora = px.box(df_sonido_box, x='hora_dia', y=col_ruido, labels={'hora_dia': 'Hora del Día', col_ruido: 'Nivel de Ruido (dB)'})
            fig_boxplot_hora.update_layout(title="Variación del Ruido a lo largo del Día")
            st.plotly_chart(fig_boxplot_hora, use_container_width=True)
        
        with col_d:
            # Tabla de Eventos de Ruido Crítico
            st.markdown("#### Eventos de Ruido Crítico (> 85 dB)")
            eventos_criticos = df_sonido[df_sonido[col_ruido] > 85].sort_values('time', ascending=False)
            st.dataframe(eventos_criticos[['time', col_ruido]].rename(columns={'time': 'Fecha y Hora', col_ruido: 'Nivel (dB)'}), use_container_width=True, height=300, hide_index=True)
    else:
        st.info("ℹ️ No se encontraron datos recientes para el sensor de Calidad del Sonido.")

# =============================
# PÁGINA: SENSORES SOTERRADOS
# =============================
elif menu == "Sensores Soterrados (EM310)":
    st.markdown("## 🌱 Sensores Soterrados - Sensor EM310")

    # Filtrar DF para solo tener datos de este sensor
    df_soterrado = df[df['object.distance'].notna()] if 'object.distance' in df.columns else pd.DataFrame()

    if not df_soterrado.empty:
        # Preparar datos para análisis de series temporales
        df_ts = df_soterrado.set_index('time').sort_index()

        col1, col2, col3 = st.columns(3)
        col1.metric("Distancia Promedio (cm)", f"{df_soterrado['object.distance'].mean():.1f}")
        
        if 'object.battery' in df_soterrado.columns:
            col2.metric("Batería Promedio (V)", f"{df_soterrado['object.battery'].mean():.1f}")
        else:
            col2.metric("Batería Promedio (V)", "N/A")

        col3.metric("Eventos Registrados", len(df_soterrado))

        st.markdown("---")
        st.markdown("### 📊 Vistas Agregadas del Estado de Contenedores")

        col_a, col_b = st.columns(2)

        with col_a:
            # Gráfico de Torta por Estado
            if 'object.status' in df_soterrado.columns:
                st.markdown("#### Estado de los Sensores")
                # Usar px.pie directamente es más robusto, agrupará los valores que encuentre.
                fig_pie_status = px.pie(df_soterrado.dropna(subset=['object.status']), names='object.status', title="Distribución por Estado")
                st.plotly_chart(fig_pie_status, use_container_width=True)

        with col_b:
            # Histograma de Niveles de Llenado
            st.markdown("#### Distribución de Niveles de Llenado")
            fig_hist_dist = px.histogram(df_soterrado, x='object.distance', nbins=20, title="Frecuencia de Niveles de Llenado (Distancia en cm)")
            st.plotly_chart(fig_hist_dist, use_container_width=True)

        # Tabla con los últimos reportes por sensor
        if 'deviceName' in df_soterrado.columns:
            st.markdown("#### Últimos Reportes por Sensor")
            # Crear la columna de etiquetas solo para la tabla, de forma segura
            if 'object.status' in df_soterrado.columns:
                df_soterrado['status_label'] = df_soterrado['object.status'].map({0: 'Normal', 1: 'Tilt', 2: 'Trigger'}).fillna('Desconocido')
            else:
                df_soterrado['status_label'] = 'N/A'
            latest_reports = df_soterrado.loc[df_soterrado.groupby('deviceName')['time'].idxmax()]
            st.dataframe(latest_reports[['deviceName', 'time', 'object.distance', 'status_label', 'object.battery']].rename(columns={'deviceName': 'Sensor', 'time': 'Último Reporte', 'object.distance': 'Distancia (cm)', 'status_label': 'Estado', 'object.battery': 'Batería (V)'}), use_container_width=True, hide_index=True)

            # Gráfico Comparativo de Evolución por Sensor
            st.markdown("#### Comparativa de Evolución por Sensor (Últimos 5 más activos)")
            # Obtener los 5 sensores con los reportes más recientes
            top_5_sensors = latest_reports.sort_values('time', ascending=False).head(5)['deviceName'].tolist()
            # Filtrar el dataframe para incluir solo los datos de esos 5 sensores
            df_top_5 = df_soterrado[df_soterrado['deviceName'].isin(top_5_sensors)]

            fig_comparativa = px.line(df_top_5, x='time', y='object.distance', color='deviceName', title="Evolución de Distancia de Múltiples Sensores", labels={'time': 'Fecha', 'object.distance': 'Distancia (cm)', 'deviceName': 'Sensor'})
            st.plotly_chart(fig_comparativa, use_container_width=True)

        st.markdown("---")
        st.markdown("### � Análisis Avanzado de la Serie Temporal (Distancia)")

        # 1. Serie de Tiempo con Media Móvil y Bandas de Bollinger
        st.markdown("#### 1. Serie de Tiempo con Media Móvil y Bandas de Bollinger")
        window_size = 7 # Media móvil de 7 periodos
        rolling_mean = df_ts['object.distance'].rolling(window=window_size).mean()
        rolling_std = df_ts['object.distance'].rolling(window=window_size).std()
        upper_band = rolling_mean + (rolling_std * 2)
        lower_band = rolling_mean - (rolling_std * 2)

        fig_bollinger = go.Figure()
        fig_bollinger.add_trace(go.Scatter(x=df_ts.index, y=df_ts['object.distance'], mode='lines', name='Distancia Original', line=dict(color='#2a9d8f')))
        fig_bollinger.add_trace(go.Scatter(x=df_ts.index, y=rolling_mean, mode='lines', name=f'Media Móvil ({window_size}p)', line=dict(color='#e76f51')))
        fig_bollinger.add_trace(go.Scatter(x=df_ts.index, y=upper_band, mode='lines', name='Banda Superior', line=dict(width=0)))
        fig_bollinger.add_trace(go.Scatter(x=df_ts.index, y=lower_band, mode='lines', name='Banda Inferior', line=dict(width=0), fill='tonexty', fillcolor='rgba(231, 111, 81, 0.2)'))
        fig_bollinger.update_layout(title="Distancia con Bandas de Bollinger", yaxis_title="Distancia (cm)")
        st.plotly_chart(fig_bollinger, use_container_width=True)

        # 2. Gráfico de Retardo (Lag Plot)
        st.markdown("#### 2. Gráfico de Retardo (Lag Plot)")
        df_lag = df_soterrado.copy()
        df_lag['distance_lag1'] = df_lag['object.distance'].shift(1)
        fig_lag = px.scatter(df_lag, x='distance_lag1', y='object.distance', title='Gráfico de Retardo (t vs t-1)', labels={'distance_lag1': 'Distancia (t-1)', 'object.distance': 'Distancia (t)'})
        fig_lag.add_shape(type="line", x0=df_lag['distance_lag1'].min(), y0=df_lag['distance_lag1'].min(), x1=df_lag['distance_lag1'].max(), y1=df_lag['distance_lag1'].max(), line=dict(color="Red",))
        st.plotly_chart(fig_lag, use_container_width=True)

        # 3. Mapa de Calor Diario/Semanal
        st.markdown("#### 3. Mapa de Calor Diario/Semanal")
        df_heatmap = df_soterrado.copy()
        df_heatmap['dia_semana'] = df_heatmap['time'].dt.day_name()
        df_heatmap['hora_dia'] = df_heatmap['time'].dt.hour
        heatmap_data = df_heatmap.pivot_table(values='object.distance', index='hora_dia', columns='dia_semana', aggfunc='mean')
        dias_ordenados = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        heatmap_data = heatmap_data.reindex(columns=dias_ordenados)
        fig_heatmap = px.imshow(heatmap_data, title="Mapa de Calor de Distancia Promedio (Hora vs. Día)", labels=dict(x="Día de la Semana", y="Hora del Día", color="Distancia (cm)"))
        st.plotly_chart(fig_heatmap, use_container_width=True)
    else:
        st.info("ℹ️ No se encontraron datos recientes para el sensor Soterrado.")
