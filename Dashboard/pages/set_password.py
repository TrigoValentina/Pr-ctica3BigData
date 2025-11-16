import streamlit as st
import bcrypt
from supabase_client import supabase

st.set_page_config(page_title="Crear Contraseña", page_icon="🔑")

# 🔥 Si no viene del login con codigo temporal
if "pending_user" not in st.session_state:
    st.error("Acceso no autorizado.")
    st.stop()

user = st.session_state["pending_user"]

st.markdown("<h2>🔑 Crear nueva contraseña</h2>", unsafe_allow_html=True)

pw1 = st.text_input("Nueva contraseña", type="password")
pw2 = st.text_input("Confirmar contraseña", type="password")

if st.button("Guardar contraseña"):
    if pw1 != pw2:
        st.error("Las contraseñas no coinciden.")
    else:
        hashed = bcrypt.hashpw(pw1.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

        supabase.table("app_users").update({
            "password_hash": hashed,
            "temp_code": None     # 🔥 Eliminar código temporal
        }).eq("id", user["id"]).execute()

        st.success("Contraseña creada. Ahora inicia sesión normalmente.")
        del st.session_state["pending_user"]
        st.switch_page("auth_app.py")
