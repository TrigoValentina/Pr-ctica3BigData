import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import numpy as np

# IMPORTANT: Importa la función que ya tienes en dashboard

# =========================================================
# MAIN FUNCTION (NECESARIA PARA EJECUTARLO DESDE dashboard.py)
# =========================================================
def main():
    from dashboards.shared import leer_datos_mysql

    # =========================================================
    # CONFIG
    # =========================================================
    st.set_page_config(page_title="Dashboard GAMC UI Nueva", page_icon="📊", layout="wide")

    st.markdown("""
    <style>
        .block-container {padding-top: 1.5rem !important;}
        .stat-box { border-left: 4px solid #2B67F6; padding-left: 10px; margin-bottom: 10px; font-size: 16px;}
        .big-title { font-size: 38px; font-weight: 700; color: #111;}
        .subtext { color: #777; margin-top: -10px; margin-bottom: 12px;}
    </style>
    """, unsafe_allow_html=True)

    # =========================================================
    # GRÁFICOS
    # =========================================================
    def plot_line(df, col):
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["time"], y=df[col], mode="lines", fill="tozeroy"))
        fig.update_layout(height=350, title="📈 Línea")
        return fig

    def plot_moving_avg(df, col):
        df["moving_avg"] = df[col].rolling(10).mean()
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df["time"], y=df[col], mode="lines", opacity=0.3))
        fig.add_trace(go.Scatter(x=df["time"], y=df["moving_avg"], mode="lines", line=dict(width=4)))
        fig.update_layout(height=350, title="📉 Promedio móvil")
        return fig

    def plot_bar(df, col):
        df["hour"] = df["time"].dt.hour
        grouped = df.groupby("hour")[col].mean()
        fig = go.Figure(go.Bar(x=grouped.index, y=grouped.values))
        fig.update_layout(height=350, title="📊 Promedio por Hora")
        return fig

    def plot_box(df, col):
        fig = go.Figure()
        fig.add_trace(go.Box(y=df[col], boxmean=True))
        fig.update_layout(height=350, title="📦 Distribución")
        return fig

    GRAPH_TYPES = [plot_line, plot_moving_avg, plot_bar, plot_box]

    # =========================================================
    # GRAPH STATE
    # =========================================================
    if "graph_index" not in st.session_state:
        st.session_state.graph_index = 0

    def graph_next(): st.session_state.graph_index = (st.session_state.graph_index + 1) % len(GRAPH_TYPES)
    def graph_prev(): st.session_state.graph_index = (st.session_state.graph_index - 1) % len(GRAPH_TYPES)

    # =========================================================
    # UI
    # =========================================================
    st.markdown('<div class="big-title">View</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtext">Visualizador unificado GAMC</div>', unsafe_allow_html=True)

    sensor = st.selectbox("Selecciona una sección", [
        "🔊 Calidad del Sonido (WS302)",
        "🌫️ Calidad del Aire (EM500)",
        "🌱 Sensores Soterrados (EM310)"
    ])

    # =========================================================
    # LOAD DATA
    # =========================================================
    if sensor == "🔊 Calidad del Sonido (WS302)":
        df = leer_datos_mysql("ws302_sonido")
        variable_options = ["LAeq","LAI","LAImax"]
    elif sensor == "🌫️ Calidad del Aire (EM500)":
        df = leer_datos_mysql("em500_co2")
        variable_options = [col for col in ["co2","temperature","humidity","pressure"] if col in df.columns]
    else:
        df = leer_datos_mysql("em310_soterrados")
        variable_options = ["distance"]

    if df.empty:
        st.warning("⚠ No hay datos disponibles para este sensor.")
        st.stop()

    df["time"] = pd.to_datetime(df["time"])

    # =========================================================
    # FILTERS (NUEVO RANGO DE FECHAS)
    # =========================================================
    st.markdown("### 📅 Filtro de fecha")

    selected_dates = st.date_input(
        "Rango de Fechas",
        [df["time"].min(), df["time"].max()],
        key="date_range2"
    )

    if isinstance(selected_dates, list) and len(selected_dates) == 2:
        fecha_inicio, fecha_fin = selected_dates
    elif isinstance(selected_dates, list) and len(selected_dates) == 1:
        fecha_inicio = fecha_fin = selected_dates[0]
    else:
        fecha_inicio = df["time"].min()
        fecha_fin = df["time"].max()

    df = df[(df["time"] >= pd.to_datetime(fecha_inicio)) & (df["time"] <= pd.to_datetime(fecha_fin))]

    if df.empty:
        st.warning("⚠ No hay datos en ese rango de fechas.")
        st.stop()

    # =========================================================
    # TOP UI
    # =========================================================
    col1, col2, col3 = st.columns([1,1,3])

    with col1: st.button("◀", on_click=graph_prev)
    with col2: st.button("▶", on_click=graph_next)
    with col3: variable = st.selectbox("Variable", variable_options)

    st.markdown("---")

    # =========================================================
    # CLEAN DATA
    # =========================================================
    df[variable] = pd.to_numeric(df[variable], errors="coerce")
    df = df[df[variable].notna()]

    # =========================================================
    # GRAPH + STATS
    # =========================================================
    left, right = st.columns([2.3, 1])

    with left:
        fig = GRAPH_TYPES[st.session_state.graph_index](df, variable)
        st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Estadísticas")
        stats = df[variable].describe()
        st.write(f"Min: {stats['min']:.2f}")
        st.write(f"Max: {stats['max']:.2f}")
        st.write(f"Prom: {stats['mean']:.2f}")
        st.write(f"Mediana: {stats['50%']:.2f}")

    # =========================================================
    # FOOTER
    # =========================================================
    st.markdown("---")
    st.caption(f"Última actualización: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
