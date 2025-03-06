#!/bin/bash
# data_ingestion/sqoop_import.sh
#
# This script imports data from Oracle tables to HDFS using Sqoop.
# It uses environment variables from oracle_connection.env and configuration variables
# for HDFS paths and Hive integration.
#
# Prerequisites:
# - Oracle JDBC driver must be installed on the edge node.
# - Oracle connection details are configured in data_ingestion/oracle_connection.env.
# - Update the BUCKET_S3A, COUNTRY_SYSTEM, and HIVE_DATABASE variables with your actual values.
#
# Usage: ./sqoop_import.sh
#
# Author: [Your Name]
# Date: [Current Date]

# Exit immediately if a command exits with a non-zero status
set -e

# Load Oracle connection variables
source "$(dirname "$0")/oracle_connection.env"

# Configuration Variables - update these with your actual values
BUCKET_S3A="your_bucket_name"    # Replace with your actual bucket name
COUNTRY_SYSTEM="co_opera"         # Update if your country/system identifier differs
HIVE_DATABASE="co_opera"          # Hive database name

# Function to perform Sqoop import for a given table
sqoop_import() {
  local table_name=$1
  local split_by_field=$2
  local boundary_query=$3
  local oracle_schema=$4  # e.g., SC4J
  echo "Starting Sqoop import for table: ${table_name}"
  sqoop import \
      --connect "jdbc:oracle:thin:@//${ORACLE_HOST}:${ORACLE_PORT}/${ORACLE_SERVICE}" \
      --username "${ORACLE_USER}" \
      --password "${ORACLE_PASS}" \
      --query "SELECT * FROM ${oracle_schema}.${table_name} WHERE \$CONDITIONS" \
      --target-dir "s3a://${BUCKET_S3A}/warehouse/tablespace/managed/hive/${COUNTRY_SYSTEM}.db/${table_name}" \
      --split-by "${split_by_field}" \
      --boundary-query "${boundary_query}" \
      --as-parquetfile \
      --compression-codec snappy \
      --num-mappers 20 \
      --delete-target-dir \
      --hive-import \
      --hive-database "${HIVE_DATABASE}" \
      --hive-table "${table_name}" \
      --direct
  echo "Completed Sqoop import for table: ${table_name}"
}

# Sqoop import for COM_ACT_ECONOMICA
sqoop_import "COM_ACT_ECONOMICA" "ID_ACT_ECONOMICA" \
  "SELECT MIN(ID_ACT_ECONOMICA), MAX(ID_ACT_ECONOMICA) FROM SC4J.COM_ACT_ECONOMICA" "SC4J"

# Sqoop import for COM_CATEGORIA
sqoop_import "COM_CATEGORIA" "ID_CATEGORIA" \
  "SELECT MIN(ID_CATEGORIA), MAX(ID_CATEGORIA) FROM SC4J.COM_CATEGORIA" "SC4J"

# Sqoop import for CRT_CUENTA
sqoop_import "CRT_CUENTA" "ID_CUENTA" \
  "SELECT MIN(ID_CUENTA), MAX(ID_CUENTA) FROM SC4J.CRT_CUENTA" "SC4J"

# Sqoop import for NUC_CLIENTE
sqoop_import "NUC_CLIENTE" "ID_CLIENTE" \
  "SELECT MIN(ID_CLIENTE), MAX(ID_CLIENTE) FROM SC4J.NUC_CLIENTE" "SC4J"

# Sqoop import for NUC_CUENTA
sqoop_import "NUC_CUENTA" "ID_CUENTA" \
  "SELECT MIN(ID_CUENTA), MAX(ID_CUENTA) FROM SC4J.NUC_CUENTA" "SC4J"

# Sqoop import for NUC_PERSONA
sqoop_import "NUC_PERSONA" "ID_PERSONA" \
  "SELECT MIN(ID_PERSONA), MAX(ID_PERSONA) FROM SC4J.NUC_PERSONA" "SC4J"

# Sqoop import for NUC_SERVICIO
sqoop_import "NUC_SERVICIO" "ID_SERVICIO" \
  "SELECT MIN(ID_SERVICIO), MAX(ID_SERVICIO) FROM SC4J.NUC_SERVICIO" "SC4J"

# Sqoop import for NUC_TIP_DOC_PERSONA
sqoop_import "NUC_TIP_DOC_PERSONA" "ID_TIP_DOC_PERSONA" \
  "SELECT MIN(ID_TIP_DOC_PERSONA), MAX(ID_TIP_DOC_PERSONA) FROM SC4J.NUC_TIP_DOC_PERSONA" "SC4J"

echo "All Sqoop imports completed successfully."