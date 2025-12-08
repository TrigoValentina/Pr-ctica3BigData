# 🔄 Después de Reiniciar el PC

**Guía rápida para continuar trabajando después de reiniciar tu computadora**

---

## ⚡ Inicio Ultra Rápido (1 Comando)

### Simplemente ejecuta:

```bash
cd c:\Users\jg012\Downloads\Pr-ctica3BigData
INICIAR.bat
```

**Eso es TODO.** El script detecta que ya está instalado y solo inicia los servicios.

⏱️ **Tiempo:** ~30 segundos

---

## 📋 Lo que hace automáticamente:

1. ✅ Verifica que Docker esté corriendo
2. ✅ Inicia los 12 contenedores
3. ✅ Verifica el estado
4. ✅ Muestra instrucciones de acceso

---

## 🌐 Acceder al Sistema

Una vez que `INICIAR.bat` termine:

```
URL: http://localhost:8501
Usuario: Oscar
Password: 1234Huicho
```

**Navega a:** 🤖 Machine Learning
- 📈 RF-01: Predicciones automáticas
- 📊 RF-02: Matriz de confusión

---

## 🛠️ Comandos Útiles

### Ver estado del sistema:
```bash
docker ps
```

### Ver logs de servicios ML:
```bash
docker logs -f ml-trainer
docker logs -f ml-predictor
```

### Generar predicciones manualmente:
```bash
generar_predicciones.bat 2025-06-01 2025-12-31
```

### Detener el sistema:
```bash
docker-compose down
```

---

## 🐛 Si Algo No Funciona

### Docker no responde:

```bash
# 1. Detener todo
docker-compose down

# 2. Reiniciar Docker Desktop
# (Cerrar y abrir Docker Desktop desde el menú de Windows)

# 3. Ejecutar de nuevo
INICIAR.bat
```

### Contenedores no inician:

```bash
# Limpiar y reiniciar
docker-compose down --remove-orphans
docker system prune -f
INICIAR.bat
```

### Dashboard no carga:

```bash
# Reiniciar solo streamlit
docker-compose restart streamlit

# O visitar directamente
http://localhost:8501
```

---

## ✅ Checklist Rápido

- [ ] Docker Desktop está corriendo (ícono en bandeja del sistema)
- [ ] Ejecuté `INICIAR.bat`
- [ ] Esperé ~30 segundos
- [ ] Vi mensaje "SISTEMA LISTO"
- [ ] Accedí a http://localhost:8501
- [ ] Login funciona (Oscar/1234Huicho)

---

## 📚 Más Información

**Para detalles completos del sistema:**  
👉 Lee [GUIA_MAESTRA.md](GUIA_MAESTRA.md)

**Para soporte:**  
👉 Ver sección Troubleshooting en GUIA_MAESTRA.md

---

## 💡 Tip

**Crea un acceso directo:**

1. Click derecho en `INICIAR.bat`
2. Enviar a → Escritorio (crear acceso directo)
3. Renombrar a "Sistema Big Data ML"

**Ahora puedes iniciar el sistema con doble click desde el escritorio!** 🎯

---

**🚀 Resumen:** Después de reiniciar → Ejecuta `INICIAR.bat` → Listo en 30 segundos
