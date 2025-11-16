import streamlit as st
import bcrypt
from supabase_client import supabase

st.set_page_config(page_title="Crear Contraseña", page_icon="🔑")

st.markdown("""
<style>
section[data-testid="stSidebar"] { display:none !important; }
div[data-testid="stAppViewContainer"] { margin-left:0 !important; }
</style>
""", unsafe_allow_html=True)

if "reset_user" not in st.session_state:
    st.error("No hay usuario para restablecer contraseña.")
    st.stop()

user = st.session_state["reset_user"]

st.title("🔑 Crear nueva contraseña")

new_pass = st.text_input("Nueva contraseña", type="password")
confirm_pass = st.text_input("Confirmar contraseña", type="password")

if st.button("Guardar contraseña"):
    if new_pass.strip() == "" or confirm_pass.strip() == "":
        st.error("Todos los campos son obligatorios.")
    elif new_pass != confirm_pass:
        st.error("Las contraseñas no coinciden.")
    else:
        hashed = bcrypt.hashpw(new_pass.encode(), bcrypt.gensalt()).decode()

        supabase.table("app_users").update({
            "password_hash": hashed,
            "temp_code": None
        }).eq("id", user["id"]).execute()

        st.success("Contraseña creada correctamente ✔")

        # limpiar sesión temporal
        del st.session_state["reset_user"]

        # Redireccionar según rol
        role = user["role"]

        if role == "admin":
            st.switch_page("admin_panel.py")
        elif role == "ejecutivo":
            st.switch_page("ejecutivo_panel.py")
        else:
            st.switch_page("../app.py")
