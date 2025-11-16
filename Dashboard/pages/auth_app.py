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


# ======================================================
# 🔐 PANTALLA PARA CREAR CONTRASEÑA DESPUÉS DEL CÓDIGO
# ======================================================
def mostrar_form_reset():
    user_reset = st.session_state.get("user_reset")

    if not user_reset:
        st.error("No hay usuario pendiente de cambio de contraseña.")
        return

    st.title("🔑 Crear nueva contraseña")
    st.write(f"Usuario: **{user_reset['username']}**")

    nueva = st.text_input("Nueva contraseña", type="password")
    confirmar = st.text_input("Confirmar contraseña", type="password")

    if st.button("Guardar contraseña"):
        if nueva.strip() == "" or confirmar.strip() == "":
            st.error("La contraseña no puede estar vacía.")
            return

        if nueva != confirmar:
            st.error("Las contraseñas no coinciden.")
            return

        # Guardar contraseña final
        hashed = bcrypt.hashpw(nueva.encode("utf-8"), bcrypt.gensalt()).decode()

        supabase.table("app_users").update({
            "password_hash": hashed,
            "temp_code": None
        }).eq("id", user_reset["id"]).execute()

        # Crear sesión JWT
        token = create_token(user_reset["id"], user_reset["username"], user_reset["role"])

        st.session_state["logged"] = True
        st.session_state["token"] = token
        st.session_state["user"] = user_reset

        # Limpiar variables de reset
        st.session_state["pending_password_reset"] = False
        del st.session_state["user_reset"]

        st.success("Contraseña creada correctamente ✔")

        # Redirigir según rol
        if user_reset["role"] == "admin":
            st.switch_page("pages/admin_panel.py")
        elif user_reset["role"] == "ejecutivo":
            st.switch_page("pages/ejecutivo_panel.py")
        else:
            st.switch_page("app.py")


# ======================================================
# 🔐 LOGIN NORMAL + CÓDIGO TEMPORAL
# ======================================================
def main():

    # Si está en modo de crear contraseña → mostrar esa pantalla
    if st.session_state.get("pending_password_reset"):
        mostrar_form_reset()
        return

    st.title("🔐 Sistema GAMC - Inicio de Sesión")

    username = st.text_input("Usuario")
    password = st.text_input("Contraseña o código temporal", type="password")

    if st.button("Ingresar"):

        # Buscar usuario
        res = supabase.table("app_users").select("*").eq("username", username).execute()
        if len(res.data) == 0:
            st.error("Usuario o contraseña/código incorrecto.")
            return

        user = res.data[0]

        temp_code = user.get("temp_code")
        primer_ingreso = temp_code not in (None, "", "null")

        # =============================
        # CASO A → Primer ingreso
        # =============================
        if primer_ingreso:

            if password == temp_code:

                # Guardar datos para el proceso de creación de contraseña
                st.session_state["pending_password_reset"] = True
                st.session_state["user_reset"] = {
                    "id": user["id"],
                    "username": user["username"],
                    "role": user["role"]
                }

                st.success("Código correcto. Ahora define tu nueva contraseña.")

                # Recargar página con el nuevo estado (SIN rerun)
                st.switch_page("pages/auth_app.py")
                return

            else:
                st.error("Código temporal incorrecto.")
                return

        # =============================
        # CASO B → Login normal
        # =============================
        else:

            if bcrypt.checkpw(password.encode("utf-8"), user["password_hash"].encode("utf-8")):

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
                st.error("Usuario o contraseña/código incorrecto.")



# Ejecutar app
if __name__ == "__main__":
    main()
