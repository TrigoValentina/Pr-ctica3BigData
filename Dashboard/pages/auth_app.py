import streamlit as st
import bcrypt
from supabase_client import supabase
from jwt_utils import create_token

st.set_page_config(page_title="Login GAMC", page_icon="🔐")

# Ocultar sidebar
st.markdown("""
<style>
section[data-testid="stSidebar"] { display: none !important; }
div[data-testid="stAppViewContainer"] { margin-left: 0 !important; }
</style>
""", unsafe_allow_html=True)

def login(username, password):
    res = supabase.table("app_users").select("*").eq("username", username).execute()
    if len(res.data) == 0:
        return None
    user = res.data[0]

    if user["password_hash"] is None:
        return "reset_needed"  # primera vez sin contraseña

    if bcrypt.checkpw(password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        return user

    return None

# ==========================================
# LOGIN UI
# ==========================================
st.title("🔐 Sistema GAMC - Inicio de Sesión")
username = st.text_input("Usuario")
password = st.text_input("Contraseña", type="password")

if st.button("Ingresar"):
    user = login(username, password)

    if user == "reset_needed":
        st.warning("Tu cuenta requiere crear contraseña primero.")
        st.switch_page("pages/reset_password.py")

    elif user:
        token = create_token(user["id"], user["username"], user["role"])

        st.session_state["logged"] = True
        st.session_state["token"] = token
        st.session_state["user"] = user

        st.success("Inicio de sesión exitoso ✔")

        if user["role"] == "admin":
            st.switch_page("pages/admin_panel.py")
        elif user["role"] == "ejecutivo":
            st.switch_page("pages/ejecutivo_panel.py")
        else:
            st.switch_page("app.py")

    else:
        st.error("Usuario o contraseña incorrectos")
