# 🌟 Sistema Big Data + Machine Learning - IoT Pipeline

**Proyecto completo de procesamiento Big Data con Machine Learning predictivo para datos IoT**

[![Estado](https://img.shields.io/badge/Estado-Operacional-success)](https://github.com)
[![Versión](https://img.shields.io/badge/Versión-1.0-blue)](https://github.com)
[![Docker](https://img.shields.io/badge/Docker-Requerido-2496ED?logo=docker)](https://www.docker.com/)

---
## 🚀 Documentacion:
https://univalleedu-my.sharepoint.com/:w:/g/personal/aio0032953_est_univalle_edu/IQBDHOShJg3ZQ7IyEJVUJBcUARD8xt1Qngd0anxSF0Tf8Rg?e=StOK68
https://univalleedu-my.sharepoint.com/:w:/g/personal/tme0032929_est_univalle_edu/IQDSLbf0ROu3TII7ZOHqppA6AXl1Kwtsw7XH0SIhQhRaOFk?rtime=Z2tosmA23kg
## ⚡ Inicio Ultra Rápido (1 Comando)

### 🚀 **Ejecuta UN SOLO script que hace TODO:**

```bash
cd c:\Users\jg012\Downloads\Pr-ctica3BigData
INICIAR.bat
```

**Este script es inteligente:**
- ✅ Si es **primera vez** → Instala todo automáticamente (5-10 min)
- ✅ Si ya está **instalado** → Solo inicia el sistema (30 segundos)
- ✅ Verifica estado y muestra instrucciones

**¡Un solo comando para TODO!** 🎉

---

## 📘 Documentación

### 🎯 **[GUIA_MAESTRA.md](GUIA_MAESTRA.md)** ← Documentación Completa

**Contiene:**
- Arquitectura completa
- Cómo funciona cada componente
- Machine Learning explicado
- Dashboard (RF-01 y RF-02)
- Troubleshooting
- Referencia técnica

### Documentación Adicional:
- **[DESPUES_DE_REINICIAR.md](DESPUES_DE_REINICIAR.md)** - Guía post-reinicio (ejecuta `INICIAR.bat`)

---

## 🎯 Acceso Rápido

Después de ejecutar `INICIAR.bat`:

```
URL: http://localhost:8501
Usuario: Oscar
Password: 1234Huicho
```

**Navega a:** 🤖 Machine Learning
- **📈 RF-01:** Regresión dinámica (genera predicciones automáticamente)
- **📊 RF-02:** Matriz de confusión

---

## 🏗️ Arquitectura

```
CSV → Kafka → Spark → MySQL
                        ↓
                   ML-Trainer (24h)
                        ↓
                   ML-Predictor (1h)
                        ↓
                   Dashboard (RF-01/RF-02)
```

**12 Contenedores Docker:**
- 10 Big Data (Kafka, Spark, MySQL, MongoDB, etc.)
- 2 ML (ml-trainer, ml-predictor)

---

## 🤖 Machine Learning

### Predicciones On-Demand ⭐

1. Selecciona **cualquier rango de fechas** en el dashboard
2. Sistema detecta que no hay predicciones
3. **Genera automáticamente en 20-30 segundos**
4. Muestra gráficas con variación realista

**Características:**
- Patrones horarios (día/noche)
- Patrones semanales (laboral/fin de semana)
- Tendencias naturales
- Variación aleatoria (±5%)

### Modelos Implementados

| Sensor | Tipo | Métricas | Evaluación |
|--------|------|----------|------------|
| EM500 | Regresión | co2, temp, humidity, pressure | R², RMSE, MAE |
| WS302 | Regresión | LAeq, LAI, LAImax | R², RMSE, MAE |
| EM310 | Regresión | distance | R², RMSE, MAE |
| EM310 | Clasificación | Normal/Alerta/Crítico | Accuracy, Precision, Recall, F1 |

---

## 📊 RF-01 y RF-02

### RF-01: Generación Dinámica de Gráficas ✅

```
Dashboard → ML → Regresión (RF-01)
├─ Selector de fechas (cualquier rango)
├─ Selector de sensor/métrica
├─ Gráfica Real vs Predicción
├─ Métricas R², RMSE, MAE (dinámicas por rango)
├─ Distribución de error
├─ Comparación con métricas globales
└─ Generación automática si no hay datos ⭐
```

### RF-02: Matriz de Confusión ✅

```
Dashboard → ML → Clasificación (RF-02)
├─ Matriz de confusión 3x3 (heatmap)
├─ Métricas: Accuracy, Precision, Recall, F1
├─ Detalles VP/FP/VN/FN por clase
├─ Descarga CSV
└─ Comparación con métricas globales
```

---

## 🛠️ Comandos Útiles

### Comandos Principales

```bash
# Iniciar/Instalar (TODO en 1 comando)
INICIAR.bat

# Detener sistema
docker-compose down

# Ver estado
docker ps

# Ver logs ML
docker logs -f ml-trainer
docker logs -f ml-predictor
```

### Comandos ML

```bash
# Entrenar modelos manualmente
run_ml_train_regression.bat
run_ml_train_classification.bat

# Generar predicciones para rango custom
generar_predicciones.bat 2025-06-01 2025-12-31
```

---

## 📁 Estructura del Proyecto

```
Pr-ctica3BigData/
│
├── 🚀 INICIAR.bat               ⭐ SCRIPT MAESTRO (ejecuta esto)
├── 📘 GUIA_MAESTRA.md            Documentación completa
├── 📄 README.md                  Este archivo
│
├── 🐳 docker-compose.yml         12 servicios
├── 🐳 Dockerfile                 Imagen custom
│
├── app/
│   ├── etl/                     Producers + Consumers
│   └── ml/                      Machine Learning
│       ├── quick_predictor.py   (on-demand) ⭐
│       └── ...otros 6 scripts
│
├── dashboards/
│   └── dashboard.py             Streamlit (RF-01 + RF-02)
│
├── data/                        CSVs sensores
├── sql/                         Scripts SQL
├── storage/ml_models/           Modelos entrenados
│
└── Scripts .bat                 Automatización
    ├── INICIAR.bat              ⭐ Ejecuta primero
    ├── generar_predicciones.bat
    └── ...otros 4 scripts
```

---

## 📊 Estado del Sistema

| Componente | Cantidad | Estado |
|------------|----------|--------|
| **Contenedores** | 12 | ✅ |
| **Registros** | ~390,000 | ✅ |
| **Modelos ML** | 4 | ✅ |
| **Predicciones** | On-Demand | ✅ |
| **RF-01** | Implementado | ✅ |
| **RF-02** | Implementado | ✅ |

---

## 🐛 Troubleshooting

| Problema | Solución |
|----------|----------|
| INICIAR.bat falla | 1. Verifica Docker Desktop está corriendo<br>2. `docker-compose down`<br>3. Ejecuta `INICIAR.bat` de nuevo |
| Dashboard no carga | `docker-compose restart streamlit` |
| Predicciones no generan | `generar_predicciones.bat` manual |
| Sin datos | Los producers se ejecutan automáticamente con consumers |

**Troubleshooting detallado:** Ver [GUIA_MAESTRA.md](GUIA_MAESTRA.md)

---

## ✅ Checklist

Después de ejecutar `INICIAR.bat`:

- [ ] 12 contenedores corriendo (`docker ps`)
- [ ] Dashboard accesible (http://localhost:8501)
- [ ] Login funciona (Oscar/1234Huicho)
- [ ] ML section visible
- [ ] RF-01 genera predicciones on-demand
- [ ] RF-02 muestra matriz de confusión

---

## 🎯 Flujo de Uso Típico

### Primera Vez (Instalación):

```bash
# 1. Ejecutar script maestro
INICIAR.bat

# Esperar 5-10 minutos (instalación completa)

# 2. Acceder dashboard
http://localhost:8501

# 3. Explorar ML
Machine Learning → RF-01 o RF-02
```

### Uso Diario:

```bash
# 1. Iniciar (solo 30 segundos)
INICIAR.bat

# 2. Acceder dashboard
http://localhost:8501

# 3. Usar predicciones on-demand
Seleccionar fechas → Automático ⭐
```

---

## 📞 Información

**Versión:** 1.0 Final  
**Fecha:** 2025-12-06  
**Estado:** ✅ 100% OPERACIONAL

---

## 🚀 Ventajas del Sistema

✅ **Un solo comando** para instalar o iniciar (`INICIAR.bat`)  
✅ **Completamente automático** (ML-trainer cada 24h, ML-predictor cada 1h)  
✅ **Predicciones on-demand** (sin scripts manuales)  
✅ **Predicciones realistas** (variación temporal natural)  
✅ **RF-01 y RF-02** completamente funcionales  
✅ **Documentación completa** (GUIA_MAESTRA.md)  
✅ **Producción ready** (sistema autónomo 24/7)

---

**🎉 El sistema más simple de usar:** Solo ejecuta `INICIAR.bat` y accede a http://localhost:8501

**📖 Para dominar el sistema:** Lee [GUIA_MAESTRA.md](GUIA_MAESTRA.md)
