# app/ml/ml_config.py
"""
Configuración centralizada para Machine Learning
"""

from datetime import datetime

# ============================================
# CONFIGURACIÓN DE MODELOS DE REGRESIÓN
# ============================================
REGRESSION_CONFIG = {
    "em500_co2": {
        "table": "em500_co2",
        "target_metrics": ["co2", "temperature", "humidity", "pressure"],
        "device_column": "device_name",
        "time_column": "time",
        "algorithm": "RandomForestRegressor",
        "hyperparameters": {
            "numTrees": 10,
            "maxDepth": 5,
            "minInstancesPerNode": 1,
            "seed": 42
        },
        "features": {
            "time_based": ["hour", "day_of_week", "month"],
            "lag_features": [1, 2, 3]  # Rolling averages
        }
    },
    "ws302_sonido": {
        "table": "ws302_sonido",
        "target_metrics": ["LAeq", "LAI", "LAImax"],
        "device_column": "tenant_name",
        "time_column": "time",
        "algorithm": "RandomForestRegressor",
        "hyperparameters": {
            "numTrees": 10,
            "maxDepth": 5,
            "minInstancesPerNode": 1,
            "seed": 42
        },
        "features": {
            "time_based": ["hour", "day_of_week", "month"],
            "lag_features": [1, 2, 3]
        }
    },
    "em310_soterrados": {
        "table": "em310_soterrados",
        "target_metrics": ["distance"],
        "device_column": "device_name",
        "time_column": "time",
        "algorithm": "RandomForestRegressor",
        "hyperparameters": {
            "numTrees": 10,
            "maxDepth": 5,
            "minInstancesPerNode": 1,
            "seed": 42
        },
        "features": {
            "time_based": ["hour", "day_of_week", "month"],
            "lag_features": [1, 2, 3]
        }
    }
}

# ============================================
# CONFIGURACIÓN DE MODELOS DE CLASIFICACIÓN
# ============================================
CLASSIFICATION_CONFIG = {
    "em310_soterrados": {
        "table": "em310_soterrados",
        "classes": {
            0: "Normal",
            1: "Alerta",
            2: "Crítico"
        },
        "thresholds": {
            "distance_high": 100,  # > 100 cm = Normal
            "distance_low": 30     # < 30 cm = Crítico
        },
        "device_column": "device_name",
        "time_column": "time",
        "algorithm": "RandomForestClassifier",
        "hyperparameters": {
            "numTrees": 10,
            "maxDepth": 5,
            "minInstancesPerNode": 1,
            "seed": 42
        },
        "features": {
            "time_based": ["hour", "day_of_week", "month"],
            "numeric": ["distance"],
            "categorical": ["status"]
        }
    }
}

# ============================================
# CONFIGURACIÓN DE BASE DE DATOS
# ============================================
DB_CONFIG = {
    "host": "mysql",
    "port": 3306,
    "database": "emergentETLVALENTINA",
    "user": "root",
    "password": "Os51t=Ag/3=B"
}

# ============================================
# CONFIGURACIÓN DE ALMACENAMIENTO
# ============================================
STORAGE_CONFIG = {
    "models_base_path": "/opt/spark/storage/ml_models",
    "predictions_days_ahead": 7  # Predecir 7 días en el futuro
}

# ============================================
# CONFIGURACIÓN DE EVALUACIÓN
# ============================================
EVALUATION_CONFIG = {
    "train_test_split": 0.8,  # 80% training, 20% testing
    "min_samples_required": 100,  # Mínimo de muestras para entrenar
    "regression_metrics": ["r2", "rmse", "mae"],
    "classification_metrics": ["accuracy", "precision", "recall", "f1"]
}

# ============================================
# FUNCIONES AUXILIARES
# ============================================
def get_model_version():
    """Genera una versión única para el modelo basada en timestamp"""
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def get_model_path(sensor_type, model_type, version):
    """Genera la ruta completa del modelo"""
    base_path = STORAGE_CONFIG["models_base_path"]
    return f"{base_path}/{sensor_type}/{model_type}/{version}"

def get_jdbc_url():
    """Retorna la URL de conexión JDBC para MySQL"""
    return f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"

def get_db_properties():
    """Retorna las propiedades de conexión a la base de datos"""
    return {
        "user": DB_CONFIG["user"],
        "password": DB_CONFIG["password"],
        "driver": "com.mysql.cj.jdbc.Driver"
    }
