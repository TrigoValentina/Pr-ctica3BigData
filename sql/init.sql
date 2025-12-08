-- ============================================
-- Base de Datos: emergentETLVALENTINA
-- Descripción: Tablas para almacenar datos de sensores ambientales
-- ============================================

USE emergentETLVALENTINA;

-- ============================================
-- Tabla: em310_soterrados
-- Descripción: Sensores soterrados EM310 (distancia, estado)
-- ============================================
CREATE TABLE IF NOT EXISTS `em310_soterrados` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `time` DATETIME,
    `device_name` VARCHAR(255),
    `Address` VARCHAR(255),
    `Location` VARCHAR(255),
    `distance` VARCHAR(255),
    `status` VARCHAR(255),
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_time` (`time`),
    INDEX `idx_device_name` (`device_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Tabla: em500_co2
-- Descripción: Sensores de calidad del aire EM500 (CO2, temperatura, humedad, presión)
-- ============================================
CREATE TABLE IF NOT EXISTS `em500_co2` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `time` DATETIME,
    `device_name` VARCHAR(255),
    `Address` VARCHAR(255),
    `Location` VARCHAR(255),
    `co2` VARCHAR(255),
    `co2_status` VARCHAR(255),
    `co2_message` VARCHAR(255),
    `temperature` VARCHAR(255),
    `temperature_status` VARCHAR(255),
    `temperature_message` VARCHAR(255),
    `humidity` VARCHAR(255),
    `humidity_status` VARCHAR(255),
    `humidity_message` VARCHAR(255),
    `pressure` VARCHAR(255),
    `pressure_status` VARCHAR(255),
    `pressure_message` VARCHAR(255),
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_time` (`time`),
    INDEX `idx_device_name` (`device_name`),
    INDEX `idx_co2_status` (`co2_status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Tabla: ws302_sonido
-- Descripción: Sensores de calidad del sonido WS302 (LAeq, LAI, LAImax)
-- ============================================
CREATE TABLE IF NOT EXISTS `ws302_sonido` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `time` DATETIME,
    `tenant_name` VARCHAR(255),
    `Address` VARCHAR(255),
    `Location` VARCHAR(255),
    `LAeq` VARCHAR(255),
    `LAI` VARCHAR(255),
    `LAImax` VARCHAR(255),
    `status` VARCHAR(255),
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_time` (`time`),
    INDEX `idx_tenant_name` (`tenant_name`),
    INDEX `idx_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Tabla: otros
-- Descripción: Tabla genérica para sensores no reconocidos
-- ============================================
CREATE TABLE IF NOT EXISTS `otros` (
    `id` INT AUTO_INCREMENT PRIMARY KEY,
    `time` DATETIME,
    `device_name` VARCHAR(255),
    `tenant_name` VARCHAR(255),
    `Address` VARCHAR(255),
    `Location` VARCHAR(255),
    `data_json` JSON,
    `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_time` (`time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================
-- Verificación de tablas creadas
-- ============================================
SHOW TABLES;

