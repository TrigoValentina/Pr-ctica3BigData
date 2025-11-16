import streamlit as st
from supabase_client import supabase
from jwt_utils import verify_token
import bcrypt
import pandas as pd
import random
import string

st.set_page_config(page_title="Admin GAMC", page_icon="🛠", layout="wide")

# 🔥 Ocultar sidebar
st.markdown("""
<style>
section[data-testid="stSidebar"] { display:none !important; }
div[data-testid="stAppViewContainer"] { margin-left:0 !important; }

/* Estilo tabla */
div[data-testid="dataframe"] table { background:#1e1e1e !important; color:#fff !important; }
div[data-testid="dataframe"] th { background:#2b2b2b !important; color:#fff !important; }
div[data-testid="dataframe"] td { background:#1e1e1e !important; color:#ddd !important; }

/* Logout */
.top-bar { display:flex; justify-content:flex-end; margin-bottom:10px; }
.top-bar button { background:#e74c3c !important; color:white !important; 
                  border-radius:6px; padding:6px 14px; font-weight:bold; }

/* Titles */
.title { font-size:32px; font-weight:bold; color:#f1f1f1; }
.section-title { font-size:24px; font-weight:bold; color:#f1f1f1; margin-top:25px; }
</style>
""", unsafe_allow_html=True)



# ================================
# 🔐 VALIDAR SESIÓN + JWT
# ================================
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

# Solo admin
if decoded.get("role") != "admin":
    st.error("Acceso denegado.")
    st.stop()



# ================================
# 🔘 BOTÓN LOGOUT
# ================================
top1, top2 = st.columns([8,2])
with top2:
    st.markdown('<div class="top-bar">', unsafe_allow_html=True)
    if st.button("🔒 Cerrar sesión", key="logout_admin"):
        st.session_state.clear()
        st.switch_page("pages/auth_app.py")
    st.markdown("</div>", unsafe_allow_html=True)



# ================================
# 🧭 TÍTULO
# ================================
st.markdown("<div class='title'>🛠 Panel de Administración</div>", unsafe_allow_html=True)
st.write("---")



# ================================
# 📋 LISTA DE USUARIOS
# ================================
st.markdown("<div class='section-title'>📋 Usuarios Registrados</div>", unsafe_allow_html=True)

response = supabase.table("app_users").select("*").execute()
df = pd.DataFrame(response.data)

if not df.empty:
    df = df[["id", "username", "email", "role", "created_at"]]
    df.columns = ["ID", "Nombre", "Correo Electrónico", "Rol", "Fecha de creación"]

    # Encabezado
    header_cols = st.columns([3, 4, 2, 3, 2, 2])
    header_cols[0].markdown("**Nombre**")
    header_cols[1].markdown("**Correo Electrónico**")
    header_cols[2].markdown("**Rol**")
    header_cols[3].markdown("**Fecha de creación**")
    header_cols[4].markdown("**Editar**")
    header_cols[5].markdown("**Eliminar**")

    # Filas
    for idx, row in df.iterrows():
        col1, col2, col3, col4, col5, col6 = st.columns([3, 4, 2, 3, 2, 2])

        col1.write(row["Nombre"])
        col2.write(row["Correo Electrónico"])
        col3.write(row["Rol"])
        col4.write(row["Fecha de creación"])

        if col5.button("✏️", key=f"edit_{row['ID']}"):
            st.session_state["editing_user"] = row.to_dict()
            st.experimental_rerun()

        if col6.button("🗑️", key=f"delete_{row['ID']}"):
            st.session_state["delete_user"] = row.to_dict()
            st.experimental_rerun()

else:
    st.info("No hay usuarios registrados.")



# ================================
# ✏ FORMULARIO EDITAR USUARIO
# ================================
if "editing_user" in st.session_state:
    st.write("---")
    st.subheader("✏ Editar Usuario")

    u = st.session_state["editing_user"]

    new_username = st.text_input("Nombre", u["Nombre"])
    new_email = st.text_input("Correo Electrónico", u["Correo Electrónico"])
    new_role = st.selectbox("Rol", ["operador", "ejecutivo", "admin"],
                            index=["operador", "ejecutivo", "admin"].index(u["Rol"]))
    new_password = st.text_input("Nueva contraseña (opcional)", type="password")

    if st.button("Guardar cambios ✔"):
        update_data = {
            "username": new_username,
            "email": new_email,
            "role": new_role,
        }

        if new_password.strip():
            hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
            update_data["password_hash"] = hashed

        supabase.table("app_users").update(update_data).eq("id", u["ID"]).execute()

        st.success("Usuario actualizado correctamente ✔")
        del st.session_state["editing_user"]
        st.experimental_rerun()

    if st.button("Cancelar"):
        del st.session_state["editing_user"]
        st.experimental_rerun()



# ================================
# 🗑 CONFIRMAR ELIMINAR
# ================================
if "delete_user" in st.session_state:
    st.write("---")
    d = st.session_state["delete_user"]

    st.error(f"¿Eliminar definitivamente a **{d['Nombre']}**?")

    colD1, colD2 = st.columns(2)

    if colD1.button("Sí, eliminar ahora"):
        supabase.table("app_users").delete().eq("id", d["ID"]).execute()
        st.success("Usuario eliminado ✔")
        del st.session_state["delete_user"]
        st.experimental_rerun()

    if colD2.button("Cancelar"):
        del st.session_state["delete_user"]
        st.experimental_rerun()



# ================================
# ➕ CREAR NUEVO USUARIO
# ================================
st.write("---")
st.markdown("<div class='section-title'>➕ Crear Nuevo Usuario</div>", unsafe_allow_html=True)

col1, col2, col3 = st.columns(3)
with col1:
    username = st.text_input("👤 Nombre")
with col2:
    email = st.text_input("📧 Correo")
with col3:
    role = st.selectbox("🛡 Rol", ["operador", "ejecutivo", "admin"])

password = st.text_input("🔑 Contraseña (temporal)", type="password")

if st.button("Crear usuario"):
    if username == "" or email == "" or password == "":
        st.error("Todos los campos son obligatorios.")
    else:
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

        supabase.table("app_users").insert({
            "username": username,
            "email": email,
            "role": role,
            "password_hash": hashed,
        }).execute()

        st.success("✔ Usuario creado correctamente")
        st.experimental_rerun()
