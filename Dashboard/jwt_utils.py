# jwt_utils.py
import jwt
import datetime

# ⚠️ CAMBIA ESTA CLAVE POR UNA LARGA Y SECRETA
SECRET_KEY = "CAMBIA_ESTA_CLAVE_POR_ALGO_LARGO_Y_UNICO"

ALGORITHM = "HS256"
TOKEN_HOURS_VALID = 2  # horas de sesión


def create_token(user_id: int, username: str, role: str) -> str:
    """Genera un JWT para el usuario."""
    now = datetime.datetime.utcnow()
    payload = {
        "sub": user_id,
        "username": username,
        "role": role,
        "iat": now,
        "exp": now + datetime.timedelta(hours=TOKEN_HOURS_VALID),
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    # En pyjwt 2.x ya devuelve str
    return token


def verify_token(token: str):
    """Devuelve el payload si el token es válido, o None si es inválido/expirado."""
    try:
        decoded = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return decoded
    except jwt.ExpiredSignatureError:
        # Token expirado
        return None
    except jwt.InvalidTokenError:
        # Token manipulado o inválido
        return None
