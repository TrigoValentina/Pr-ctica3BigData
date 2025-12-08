# Script de instalación de dependencias
# Ejecutar este script para instalar las librerías necesarias

Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Instalando dependencias del proyecto" -ForegroundColor Cyan
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""

# Verificar si existe requirements.txt
if (Test-Path "requirements.txt") {
    Write-Host "✅ Encontrado requirements.txt" -ForegroundColor Green
    Write-Host ""
    Write-Host "Instalando paquetes..." -ForegroundColor Yellow
    
    pip install -r requirements.txt
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host ""
        Write-Host "✅ ¡Instalación completada exitosamente!" -ForegroundColor Green
    } else {
        Write-Host ""
        Write-Host "❌ Error durante la instalación" -ForegroundColor Red
    }
} else {
    Write-Host "❌ No se encontró requirements.txt" -ForegroundColor Red
}

Write-Host ""
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host "Dependencias principales instaladas:" -ForegroundColor Cyan
Write-Host "- streamlit (Dashboard framework)" -ForegroundColor White
Write-Host "- supabase (Autenticación y base de datos)" -ForegroundColor White
Write-Host "- bcrypt (Encriptación de contraseñas)" -ForegroundColor White
Write-Host "- pandas, plotly (Visualización de datos)" -ForegroundColor White
Write-Host "- mysql-connector-python (MySQL)" -ForegroundColor White
Write-Host "- pymongo (MongoDB)" -ForegroundColor White
Write-Host "=====================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Para ejecutar el dashboard:" -ForegroundColor Yellow
Write-Host "  cd dashboards" -ForegroundColor White
Write-Host "  streamlit run dashboard.py" -ForegroundColor White
Write-Host ""
