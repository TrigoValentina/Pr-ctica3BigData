"""
Script para generar contraseñas hasheadas con bcrypt
Usar este script para crear hashes de contraseñas antes de insertarlas en Supabase
"""

import bcrypt

def generar_hash(password: str) -> str:
    """
    Genera un hash bcrypt de una contraseña
    
    Args:
        password: Contraseña en texto plano
        
    Returns:
        Hash de la contraseña listo para almacenar en la base de datos
    """
    # Generar salt y hashear la contraseña
    password_bytes = password.encode('utf-8')
    salt = bcrypt.gensalt()
    password_hash = bcrypt.hashpw(password_bytes, salt)
    
    # Decodificar a string para almacenar en DB
    return password_hash.decode('utf-8')

def verificar_password(password: str, password_hash: str) -> bool:
    """
    Verifica si una contraseña coincide con su hash
    
    Args:
        password: Contraseña en texto plano
        password_hash: Hash almacenado en la base de datos
        
    Returns:
        True si la contraseña es correcta, False si no
    """
    try:
        return bcrypt.checkpw(
            password.encode('utf-8'),
            password_hash.encode('utf-8')
        )
    except Exception as e:
        print(f"Error al verificar contraseña: {e}")
        return False

if __name__ == "__main__":
    print("=" * 60)
    print("Generador de Contraseñas Hasheadas con bcrypt")
    print("=" * 60)
    print()
    
    # Obtener contraseña del usuario
    password = input("Ingresa la contraseña a hashear: ")
    
    if not password:
        print("❌ Error: La contraseña no puede estar vacía")
        exit(1)
    
    # Generar hash
    print("\n🔒 Generando hash...")
    password_hash = generar_hash(password)
    
    print("\n✅ Hash generado exitosamente:")
    print("-" * 60)
    print(password_hash)
    print("-" * 60)
    
    # Verificar que funciona
    print("\n🔍 Verificando hash...")
    if verificar_password(password, password_hash):
        print("✅ Verificación exitosa: el hash es correcto")
    else:
        print("❌ Error: el hash no coincide con la contraseña")
    
    print("\n📋 Ejemplo de INSERT para Supabase:")
    print("-" * 60)
    print("INSERT INTO app_users (username, password_hash, role, is_active)")
    print(f"VALUES ('nombre_usuario', '{password_hash}', 'operador', true);")
    print("-" * 60)
    
    print("\n💡 Recuerda:")
    print("  1. Copia el hash generado arriba")
    print("  2. Insértalo en Supabase usando SQL Editor")
    print("  3. Reemplaza 'nombre_usuario' con el username deseado")
    print("  4. Elige el rol: 'operador' o 'ejecutivo'")
    print()
