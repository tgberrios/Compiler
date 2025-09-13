-- Table DDL for SportBook.usuarios
-- Engine: MariaDB
-- Database: SportBook
-- Generated: 1757729304

CREATE TABLE `usuarios` (
  `id` int(11) NOT NULL DEFAULT 0,
  `nombre` varchar(50) DEFAULT NULL,
  `apellido` varchar(50) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `fecha_registro` date DEFAULT NULL,
  `activo` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
