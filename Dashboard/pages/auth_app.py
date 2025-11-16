import streamlit as st
import bcrypt
from supabase_client import supabase
from jwt_utils import create_token

st.set_page_config(page_title="Login GAMC", page_icon="🔐")
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


def login(username, password):
    res = supabase.table("app_users").select("*").eq("username", username).execute()

    if len(res.data) == 0:
        return None

    user = res.data[0]

    if bcrypt.checkpw(password.encode("utf-8"), user["password_hash"].encode("utf-8")):
        return user
    return None


def show_login():
    st.markdown(
        "<h1 style='text-align:center;'>🔐 Sistema GAMC - Inicio de Sesión</h1>",
        unsafe_allow_html=True,
    )

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña", type="password")

    if st.button("Ingresar"):
        user = login(username, password)

        if user:
            # Crear JWT
            token = create_token(user["id"], user["username"], user["role"])

            # Guardar sesión
            st.session_state["logged"] = True
            st.session_state["token"] = token
            st.session_state["user"] = {
                "id": user["id"],
                "username": user["username"],
                "role": user["role"],
            }

            role = user["role"]

            st.success("Inicio de sesión exitoso")

            # 🔥 Redirección según rol (rutas siempre relativas al script principal app.py)
            if role == "admin":
                st.switch_page("pages/admin_panel.py")
            elif role == "ejecutivo":
                st.switch_page("pages/ejecutivo_panel.py")
            else:
                st.switch_page("app.py")  # operador
        else:
            st.error("Usuario o contraseña incorrectos")


if __name__ == "__main__":
    show_login()
