@echo off
cd /d "%~dp0"
echo ========================================
echo Dashboard Ambiental - GAMC
echo ========================================
echo.
echo Verificando conexion a MySQL...
python -c "import mysql.connector; conn = mysql.connector.connect(host='localhost', port=3307, database='emergentETLVALENTINA', user='root', password='Os51t=Ag/3=B'); print('✓ MySQL conectado correctamente'); conn.close()" 2>nul || echo "⚠ Advertencia: No se pudo conectar a MySQL. Verifica que este corriendo."
echo.
echo Iniciando Streamlit en http://localhost:8501
echo Presiona Ctrl+C para detener el servidor
echo.
streamlit run dashboard.py --server.port 8501 --server.address localhost
pause

