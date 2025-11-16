import streamlit as st
from jwt_utils import verify_token

st.set_page_config(page_title="Dashboard GAMC", page_icon="📊")

# Ocultar sidebar
st.markdown("""
<style>
section[data-testid="stSidebar"] { display:none !important; }
div[data-testid="stAppViewContainer"] { margin-left: 0 !important; }
</style>
""", unsafe_allow_html=True)

# ================================
# VALIDACIÓN
# ================================
if "logged" not in st.session_state:
    st.switch_page("pages/auth_app.py")

decoded = verify_token(st.session_state["token"])
if decoded is None:
    st.session_state.clear()
    st.switch_page("pages/auth_app.py")

if decoded["role"] != "operador":
    st.error("Acceso exclusivo para operadores")
    st.stop()

# ================================
# LOGOUT
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
    if st.button("Cerrar sesión"):
        st.session_state.clear()
        st.switch_page("pages/auth_app.py")
    st.markdown('</div>', unsafe_allow_html=True)

# ================================
# CONTENIDO
# ================================
st.title("📟 Panel Operador")
st.info("Dashboard operador listo.")
