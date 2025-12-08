# Dashboard Ambiental - GAMC

Dashboard interactivo para visualizar datos de sensores ambientales almacenados en MySQL.

## Características

- **Calidad del Sonido (WS302)**: Visualizaciones de niveles de sonido con:
  - Evolución temporal del nivel de sonido promedio
  - Nivel de sonido promedio por sensor (gráfico de barras)
  - Distribución del nivel de sonido por sensor (box plot)
  - Patrón de ruido: hora del día vs. día de la semana (heatmap)

- **Calidad del Aire (EM500)**: Visualizaciones de calidad del aire con:
  - Evolución temporal de CO₂, temperatura, humedad y presión
  - Métricas principales

- **Sensores Soterrados (EM310)**: Visualizaciones de sensores soterrados con:
  - Evolución temporal de distancia
  - Estado de los sensores
  - Distribución por sensor

## Requisitos

- Python 3.8+
- Streamlit
- Plotly
- mysql-connector-python
- pymongo
- pandas
- numpy

## Instalación

```bash
pip install streamlit plotly mysql-connector-python pymongo pandas numpy
```

## Ejecución

### Windows
```bash
cd dashboards
run_dashboard.bat
```

### Linux/Mac
```bash
cd dashboards
streamlit run dashboard.py --server.port 8501 --server.address localhost
```

El dashboard estará disponible en: http://localhost:8501

## Configuración

El dashboard se conecta a:
- **MySQL**: `localhost:3307` (puerto mapeado desde Docker)
- **Base de datos**: `emergentETLVALENTINA`
- **Usuario**: `root`
- **Contraseña**: Configurada en el código

Para cambiar la configuración, edita las variables en `dashboard.py`:
```python
DB_HOST = "localhost"
DB_PORT = 3307
DB_NAME = "emergentETLVALENTINA"
DB_USER = "root"
DB_PASSWORD = "Os51t=Ag/3=B"
```

## Estructura de Datos

El dashboard lee datos de las siguientes tablas en MySQL:
- `ws302_sonido`: Datos de sensores de sonido
- `em500_co2`: Datos de sensores de calidad del aire
- `em310_soterrados`: Datos de sensores soterrados
- `otros`: Datos de otros sensores

## Notas

- El dashboard usa cache para optimizar las consultas (TTL de 60 segundos)
- Los datos se actualizan automáticamente al cambiar de sección
- El auto-refresh está deshabilitado por defecto para evitar sobrecarga

