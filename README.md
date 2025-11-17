## Guía de despliegue en una nueva PC

Este documento describe, paso a paso, cómo levantar toda la arquitectura (Kafka + Spark + MySQL + MongoDB Atlas + Dashboard) en un equipo limpio. Todos los comandos están pensados para ejecutarse en PowerShell / CMD dentro de la carpeta del proyecto.

> ℹ️ Si necesitas consultar el documento original proporcionado por el usuario, sigue disponible [en este enlace](https://univalleedu-my.sharepoint.com/:w:/g/personal/tme0032929_est_univalle_edu/EZTckpI6Ow5Op1xDrLByy5oBNxU4xKoCki8BKVDN2ksSIw?e=CrbtHg).

---

### 1. Requisitos previos

- **Docker Desktop** (con al menos 2 CPUs y 8 GB de RAM asignados a Docker).
- **docker-compose** (ya viene con Docker Desktop reciente).
- **Git** para clonar el repositorio.
- **Python 3.10+** (solo para ejecutar el dashboard en local).
- Acceso a internet para descargar imágenes y dependencias.

---

### 2. Clonar el repositorio y preparar los datos

```powershell
git clone <URL_DEL_REPO> Pr-ctica3BigData
cd Pr-ctica3BigData
```

Coloca los tres archivos CSV dentro de la carpeta `data/` respetando exactamente estos nombres:

- `EM310-UDL-915M soterrados nov 2024.csv`
- `EM500-CO2-915M nov 2024.csv`
- `WS302-915M SONIDO NOV 2024.csv`

---

### 3. Revisar configuraciones clave

1. **MySQL (contenedor):**
   - Usuario: `root`
   - Contraseña: `Os51t=Ag/3=B`
   - Base de datos: `emergentETLVALENTINA`

2. **MongoDB Atlas:**
   - URI configurada directamente en los consumers (`app/etl/spark_consumer_*.py`). Si cambias las credenciales, actualiza la variable `MONGO_ATLAS_URI`.

3. **docker-compose.yml:**
   - Ya incluye Zookeeper, Kafka, Spark Master/Worker, MySQL, Mongo local y **tres consumers Spark independientes** (uno por tipo de sensor).

No se requiere ninguna variable de entorno adicional; todo está versionado.

---

### 4. Levantar la infraestructura con Docker

Desde la raíz del proyecto:

```powershell
docker-compose down --volumes --remove-orphans
docker-compose up -d
```

Verifica que todos los contenedores estén en estado `Up`:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

> Espera ~30 s hasta que `mysql` termine de inicializar (el script `sql/init.sql` crea las tablas automáticamente).

---

### 5. Ejecutar los producers por tipo de sensor

Los CSV no se procesan automáticamente para evitar re-procesamientos accidentales. Ejecuta cada productor (puedes hacerlo en paralelo, pero es más fácil hacerlo uno por uno):

```powershell
# EM310 (soterrados)
docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_em310.py

# EM500 (calidad del aire)
docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_em500.py

# WS302 (sonido)
docker exec -d spark-master python3 /opt/spark/app/etl/spark_producer_ws302.py
```

Cada script recorre su CSV, envía los mensajes a Kafka y termina. Puedes revisar los logs de Kafka o de los consumers si deseas confirmar el envío.

---

### 6. Verificar que los consumers están guardando datos

Los tres consumers (`spark-consumer-em310`, `spark-consumer-em500`, `spark-consumer-ws302`) se levantan automáticamente con Docker y clasifican los registros por tabla. Para validar que MySQL se está poblando:

```powershell
docker exec mysql mysql -uroot -p"Os51t=Ag/3=B" -e ^
"USE emergentETLVALENTINA;
 SELECT 'em310_soterrados' AS tabla, COUNT(*) FROM em310_soterrados
 UNION ALL
 SELECT 'em500_co2', COUNT(*) FROM em500_co2
 UNION ALL
 SELECT 'ws302_sonido', COUNT(*) FROM ws302_sonido;"
```

También puedes inspeccionar filas de ejemplo:

```powershell
docker exec mysql mysql -uroot -p"Os51t=Ag/3=B" -e ^
"USE emergentETLVALENTINA;
 SELECT device_name, time, co2, temperature, humidity FROM em500_co2 LIMIT 5;
 SELECT tenant_name, time, LAeq, LAI, LAImax FROM ws302_sonido LIMIT 5;"
```

Los consumers escriben simultáneamente en MongoDB Atlas (colección `sensores`). Si necesitas confirmar el flujo, usa `mongosh` o la UI de Atlas.

---

### 7. Ejecutar el dashboard (Streamlit)

El dashboard corre fuera de Docker para aprovechar la misma instalación de Python que tengas en la PC:

```powershell
cd dashboards
python -m venv .venv
.venv\Scripts\activate
pip install --upgrade pip
pip install streamlit pandas mysql-connector-python plotly pymongo python-dotenv
streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
```

El dashboard lee directamente de MySQL (`localhost:3307`) y refresca cada minuto. Si prefieres usar los scripts incluidos:

- `run_dashboard.bat` (CMD)
- `run_dashboard.ps1` (PowerShell)

> Asegúrate de que Docker siga corriendo, porque el dashboard depende de la base MySQL poblada por los consumers.

---

### 8. Apagar los servicios

```powershell
docker-compose down
```

Si necesitas liberar los datos de MySQL/Mongo locales:

```powershell
docker-compose down --volumes
```

Para detener el dashboard, basta con cerrar la terminal o presionar `Ctrl+C`.

---

### 9. Problemas frecuentes

- **MySQL rechaza conexiones**: espera unos segundos tras `docker-compose up -d`; los consumers reintentan automáticamente.
- **Kafka muestra offsets pero las tablas siguen vacías**: confirma que los producers hayan terminado sin errores (`docker logs spark-master | Select-String "🚀 PRODUCTOR"`).
- **Dashboard sin datos**: verifica que `em500_co2` y `ws302_sonido` tengan registros; si los CSV ya se consumieron, vuelve a ejecutar los producers.
- **Cambiaste credenciales**: recuerda actualizar tanto `docker-compose.yml` como los scripts `spark_consumer_*.py` y `dashboards/dashboard.py`.

---

Con estos pasos deberías poder replicar todo el flujo end-to-end en cualquier PC con Docker. Cualquier ajuste adicional (nuevos sensores, dashboards, etc.) puede versionarse siguiendo la misma estructura. ¡Éxitos!
