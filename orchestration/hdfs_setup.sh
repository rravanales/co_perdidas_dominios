#!/bin/bash
# File: orchestration/hdfs_setup.sh
#
# Description:
# This script sets up the HDFS directory structure required for staging data from the Oracle source.
# It creates directories for each Oracle table under the specified base HDFS path and sets proper permissions.
#
# Usage:
#   ./hdfs_setup.sh
#
# Requirements:
# - HDFS command-line tools must be installed and configured on the edge node.
# - Update the configuration variables below with your actual values.
#
# Author: [Your Name]
# Date: [Current Date]

# Exit immediately if a command exits with a non-zero status
set -e

# Configuration Variables - update these with your actual values
BUCKET_S3A="your_bucket_name"                       # Replace with your actual bucket name
BASE_PATH="s3a://${BUCKET_S3A}/warehouse/tablespace/managed/hive"
COUNTRY_SYSTEM="co_opera"                            # Country/system identifier, e.g., co_opera
DATABASE="${COUNTRY_SYSTEM}.db"                      # Hive database directory name

# List of Oracle tables to create directories for
TABLES=("COM_ACT_ECONOMICA" "COM_CATEGORIA" "CRT_CUENTA" "NUC_CLIENTE" "NUC_CUENTA" "NUC_PERSONA" "NUC_SERVICIO" "NUC_TIP_DOC_PERSONA")

# Loop through each table and create the necessary HDFS directory structure
for table in "${TABLES[@]}"
do
    target_dir="${BASE_PATH}/${DATABASE}/${table}"
    echo "Creating HDFS directory: ${target_dir}"
    hdfs dfs -mkdir -p "${target_dir}"
    echo "Setting permissions for ${target_dir} to 755"
    hdfs dfs -chmod 755 "${target_dir}"
done

echo "HDFS directory setup complete."