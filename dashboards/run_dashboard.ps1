# Script PowerShell para ejecutar el Dashboard
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "Dashboard Ambiental - GAMC" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Cambiar al directorio del script
Set-Location $PSScriptRoot

# Verificar conexión a MySQL
Write-Host "Verificando conexión a MySQL..." -ForegroundColor Yellow
try {
    python -c "import mysql.connector; conn = mysql.connector.connect(host='localhost', port=3307, database='emergentETLVALENTINA', user='root', password='Os51t=Ag/3=B'); print('✓ MySQL conectado correctamente'); conn.close()"
    Write-Host ""
} catch {
    Write-Host "⚠ Advertencia: No se pudo conectar a MySQL. Verifica que esté corriendo." -ForegroundColor Yellow
    Write-Host ""
}

# Iniciar Streamlit
Write-Host "Iniciando Streamlit en http://localhost:8501" -ForegroundColor Green
Write-Host "Presiona Ctrl+C para detener el servidor" -ForegroundColor Yellow
Write-Host ""

# Abrir navegador después de 3 segundos
Start-Job -ScriptBlock {
    Start-Sleep -Seconds 3
    Start-Process "http://localhost:8501"
} | Out-Null

# Ejecutar Streamlit
streamlit run dashboard.py --server.port 8501 --server.address localhost

