-- Table DDL for dbo.basic_types
-- Engine: PostgreSQL
-- Database: dbo
-- Generated: 1757729304

CREATE TABLE "dbo"."basic_types" (id integer NOT NULL, name character varying(50), age integer, salary numeric, is_active boolean, created_date date, created_time time without time zone, notes text);
