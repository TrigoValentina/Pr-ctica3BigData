-- ============================================
-- Machine Learning Tables
-- Descripción: Tablas para almacenar predicciones, métricas y metadata de modelos ML
-- ============================================

USE emergentETLVALENTINA;

-- ============================================
-- Tabla: ml_predictions_regression
-- Descripción: Predicciones de modelos de regresión (valores continuos)
-- ============================================
CREATE TABLE IF NOT EXISTS `ml_predictions_regression` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `sensor_type` VARCHAR(50) NOT NULL COMMENT 'Tipo de sensor: em310_soterrados, em500_co2, ws302_sonido',
    `device_name` VARCHAR(255) COMMENT 'Nombre del dispositivo/sensor',
    `time` DATETIME NOT NULL COMMENT 'Timestamp de la predicción',
    `metric_name` VARCHAR(50) NOT NULL COMMENT 'Métrica predicha: co2, temperature, LAeq, distance, etc.',
    `real_value` DECIMAL(10,4) DEFAULT NULL COMMENT 'Valor real (si existe)',
    `predicted_value` DECIMAL(10,4) NOT NULL COMMENT 'Valor predicho por el modelo',
    `model_version` VARCHAR(50) NOT NULL COMMENT 'Versión del modelo usado',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_sensor_time` (`sensor_type`, `time`),
    INDEX `idx_metric_time` (`metric_name`, `time`),
    INDEX `idx_device_time` (`device_name`, `time`),
    INDEX `idx_model_version` (`model_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Tabla: ml_predictions_classification
-- Descripción: Predicciones de modelos de clasificación (categorías)
-- ============================================
CREATE TABLE IF NOT EXISTS `ml_predictions_classification` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `sensor_type` VARCHAR(50) NOT NULL COMMENT 'Tipo de sensor',
    `device_name` VARCHAR(255) COMMENT 'Nombre del dispositivo/sensor',
    `time` DATETIME NOT NULL COMMENT 'Timestamp de la clasificación',
    `real_class` VARCHAR(50) DEFAULT NULL COMMENT 'Clase real (si existe): Normal, Alerta, Crítico',
    `predicted_class` VARCHAR(50) NOT NULL COMMENT 'Clase predicha por el modelo',
    `confidence` DECIMAL(5,4) COMMENT 'Confianza de la predicción (0-1)',
    `model_version` VARCHAR(50) NOT NULL COMMENT 'Versión del modelo usado',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_sensor_time` (`sensor_type`, `time`),
    INDEX `idx_predicted_class` (`predicted_class`),
    INDEX `idx_device_time` (`device_name`, `time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Tabla: ml_metrics_regression
-- Descripción: Métricas de evaluación para modelos de regresión (R², RMSE, MAE)
-- ============================================
CREATE TABLE IF NOT EXISTS `ml_metrics_regression` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `sensor_type` VARCHAR(50) NOT NULL COMMENT 'Tipo de sensor',
    `metric_name` VARCHAR(50) NOT NULL COMMENT 'Métrica evaluada: co2, LAeq, etc.',
    `model_version` VARCHAR(50) NOT NULL COMMENT 'Versión del modelo',
    `date_from` DATE NOT NULL COMMENT 'Fecha inicio del rango de evaluación',
    `date_to` DATE NOT NULL COMMENT 'Fecha fin del rango de evaluación',
    `r2_score` DECIMAL(6,4) COMMENT 'Coeficiente de determinación R²',
    `rmse` DECIMAL(10,4) COMMENT 'Root Mean Squared Error',
    `mae` DECIMAL(10,4) COMMENT 'Mean Absolute Error',
    `sample_count` INT COMMENT 'Cantidad de muestras evaluadas',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_sensor_metric` (`sensor_type`, `metric_name`),
    INDEX `idx_date_range` (`date_from`, `date_to`),
    INDEX `idx_model_version` (`model_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Tabla: ml_metrics_classification
-- Descripción: Métricas de evaluación para modelos de clasificación (Precision, Recall, F1)
-- ============================================
CREATE TABLE IF NOT EXISTS `ml_metrics_classification` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `sensor_type` VARCHAR(50) NOT NULL COMMENT 'Tipo de sensor',
    `model_version` VARCHAR(50) NOT NULL COMMENT 'Versión del modelo',
    `date_from` DATE NOT NULL COMMENT 'Fecha inicio del rango de evaluación',
    `date_to` DATE NOT NULL COMMENT 'Fecha fin del rango de evaluación',
    `accuracy` DECIMAL(6,4) COMMENT 'Exactitud del modelo',
    `precision_score` DECIMAL(6,4) COMMENT 'Precisión (weighted average)',
    `recall_score` DECIMAL(6,4) COMMENT 'Recall (weighted average)',
    `f1_score` DECIMAL(6,4) COMMENT 'F1-Score (weighted average)',
    `confusion_matrix_json` JSON COMMENT 'Matriz de confusión en formato JSON',
    `sample_count` INT COMMENT 'Cantidad de muestras evaluadas',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_sensor_type` (`sensor_type`),
    INDEX `idx_date_range` (`date_from`, `date_to`),
    INDEX `idx_model_version` (`model_version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Tabla: ml_models_metadata
-- Descripción: Metadata de modelos entrenados (versiones, hiperparámetros, rutas)
-- ============================================
CREATE TABLE IF NOT EXISTS `ml_models_metadata` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `sensor_type` VARCHAR(50) NOT NULL COMMENT 'Tipo de sensor',
    `model_type` ENUM('regression', 'classification') NOT NULL COMMENT 'Tipo de modelo',
    `model_version` VARCHAR(50) NOT NULL UNIQUE COMMENT 'Versión única del modelo (timestamp)',
    `target_metric` VARCHAR(50) COMMENT 'Métrica objetivo para regresión (co2, LAeq, etc.)',
    `training_date` DATETIME NOT NULL COMMENT 'Fecha de entrenamiento',
    `training_samples` INT COMMENT 'Cantidad de muestras usadas en entrenamiento',
    `model_path` VARCHAR(500) COMMENT 'Ruta del modelo guardado en storage',
    `hyperparameters_json` JSON COMMENT 'Hiperparámetros del modelo',
    `is_active` BOOLEAN DEFAULT TRUE COMMENT 'Si es el modelo activo para predicciones',
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_sensor_model_type` (`sensor_type`, `model_type`),
    INDEX `idx_is_active` (`is_active`),
    INDEX `idx_training_date` (`training_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Verificación de tablas ML creadas
-- ============================================
SHOW TABLES LIKE 'ml_%';

SELECT 'Tablas ML creadas exitosamente' AS status;
