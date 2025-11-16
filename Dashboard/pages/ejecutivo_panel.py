import streamlit as st
from jwt_utils import verify_token

st.set_page_config(page_title="Panel Ejecutivo", page_icon="📊")
# 🔥 Ocultar completamente la sidebar de Streamlit
hide_menu_style = """
<style>
/* Oculta la sidebar entera */
section[data-testid="stSidebar"] {
    display: none !important;
}

/* Expande el contenido a todo el ancho */
div[data-testid="stAppViewContainer"] {
    margin-left: 0 !important;
}
</style>
"""
st.markdown(hide_menu_style, unsafe_allow_html=True)

# 🔐 Verificar sesión + token
if "logged" not in st.session_state or not st.session_state["logged"]:
    st.switch_page("pages/auth_app.py")

if "token" not in st.session_state:
    st.session_state.clear()
    st.switch_page("pages/auth_app.py")

decoded = verify_token(st.session_state["token"])
if decoded is None:
    st.error("Sesión expirada. Inicie sesión nuevamente.")
    st.session_state.clear()
    st.switch_page("pages/auth_app.py")

# 🔑 Verificar rol
if decoded.get("role") != "ejecutivo":
    st.error("Acceso denegado")
    st.stop()

# Estilos para botón logout
st.markdown(
    """
<style>
    .top-bar {
        display: flex;
        justify-content: flex-end;
        align-items: center;
        margin-bottom: 0.5rem;
    }
    .top-bar button {
        background-color: #e74c3c !important;
        color: white !important;
        border-radius: 6px;
        font-weight: bold;
    }
</style>
""",
    unsafe_allow_html=True,
)

# BOTÓN LOGOUT arriba derecha
top_col1, top_col2 = st.columns([8, 2])
with top_col2:
    st.markdown('<div class="top-bar">', unsafe_allow_html=True)
    if st.button("🔒 Cerrar sesión", key="logout_ejecutivo"):
        st.session_state.clear()
        st.switch_page("pages/auth_app.py")
    st.markdown("</div>", unsafe_allow_html=True)

st.title("📈 Panel Ejecutivo - Análisis Avanzado")
st.info("Aquí luego conectamos ML, tendencias y análisis.")
