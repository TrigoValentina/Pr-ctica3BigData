# Sistema de Autenticación con Supabase

## ✅ Implementación Completada

El sistema de autenticación con Supabase ha sido migrado exitosamente del proyecto original al nuevo.

## 🔐 Características Implementadas

### 1. Autenticación con Supabase
- Base de datos PostgreSQL en la nube
- Tabla `app_users` para gestión de usuarios
- Tabla `app_logs` para auditoría de acciones

### 2. Seguridad
- ✅ Contraseñas hasheadas con **bcrypt**
- ✅ Validación de usuarios activos/inactivos
- ✅ Control de acceso por roles

### 3. Sistema de Roles
- **Operador**: Acceso al Dashboard 1 (completo)
- **Ejecutivo**: Acceso al Dashboard 2 (simplificado)

### 4. Logs de Auditoría
- Registro de login
- Registro de logout
- Almacenamiento en Supabase

## 🚀 Instalación

### Paso 1: Instalar Dependencias

```powershell
# Opción 1: Usando el script de instalación
.\install_dependencies.ps1

# Opción 2: Manualmente
pip install -r requirements.txt
```

### Paso 2: Verificar Configuración

Las credenciales de Supabase ya están configuradas en `dashboard.py`:
- **URL**: `https://ugqhpqllxrcjyusslasg.supabase.co`
- **Key**: Ya configurada (anon key)

## 📊 Estructura de Base de Datos

### Tabla: `app_users`

```sql
CREATE TABLE app_users (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  username VARCHAR UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  role VARCHAR DEFAULT 'operador',
  is_active BOOLEAN DEFAULT true,
  created_at TIMESTAMP DEFAULT NOW()
);
```

### Tabla: `app_logs`

```sql
CREATE TABLE app_logs (
  id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID REFERENCES app_users(id),
  action VARCHAR NOT NULL,
  level VARCHAR DEFAULT 'info',
  data JSONB,
  created_at TIMESTAMP DEFAULT NOW()
);
```

## 👤 Crear Usuarios

Para crear un nuevo usuario con contraseña hasheada:

```python
import bcrypt

# Generar hash de la contraseña
password = "tu_contraseña_segura"
password_hash = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

# Insertar en Supabase (desde SQL Editor):
# INSERT INTO app_users (username, password_hash, role, is_active)
# VALUES ('nombre_usuario', 'hash_generado', 'operador', true);
```

## 🔧 Uso del Sistema

### 1. Ejecutar el Dashboard

```powershell
cd dashboards
streamlit run dashboard.py
```

### 2. Iniciar Sesión

1. Abrir navegador en `http://localhost:8501`
2. Ingresar credenciales de usuario registrado en Supabase
3. El sistema validará:
   - Usuario existente
   - Usuario activo
   - Contraseña correcta
4. Redirección automática según rol

### 3. Cerrar Sesión

- Click en "🚪 Cerrar sesión" en el sidebar
- Se registra el logout en los logs

## 📝 Diferencias con el Sistema Anterior

| Característica | Anterior | Nuevo |
|---|---|---|
| **Almacenamiento** | Hardcoded | Supabase (cloud) |
| **Contraseñas** | Texto plano | Hasheadas (bcrypt) |
| **Usuarios** | 2 fijos | Ilimitados en BD |
| **Roles** | No | Sí (operador/ejecutivo) |
| **Logs** | No | Sí (en Supabase) |
| **Gestión** | Editar código | Base de datos |

## ✅ Verificación

Para verificar que todo funciona correctamente:

1. ✅ Las dependencias están instaladas (`pip list | findstr supabase`)
2. ✅ El dashboard inicia sin errores
3. ✅ El login solicita usuario y contraseña
4. ✅ Los usuarios se validan contra Supabase
5. ✅ Se muestra el rol del usuario en el sidebar
6. ✅ Los logs se registran en Supabase

## 🆘 Solución de Problemas

### Error: "ModuleNotFoundError: No module named 'supabase'"

```powershell
pip install supabase bcrypt
```

### Error: "Usuario o contraseña incorrectos" (pero son correctos)

- Verificar que el usuario existe en la tabla `app_users`
- Verificar que `is_active = true`
- Verificar que el `password_hash` está correctamente generado con bcrypt

### Error de conexión a Supabase

- Verificar conectividad a internet
- Verificar que las credenciales de Supabase son correctas
- Verificar que las tablas existen en Supabase

## 📚 Recursos

- [Documentación de Supabase](https://supabase.com/docs)
- [Documentación de bcrypt](https://github.com/pyca/bcrypt/)
- [Streamlit Authentication](https://docs.streamlit.io/)
