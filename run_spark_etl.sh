#!/bin/bash

# Script to run the Apache Spark ETL Pipeline
# Make this file executable with: chmod +x run_spark_etl.sh

echo "==================== SPARK ETL PIPELINE ===================="
echo "Setting up environment..."

# Create the PostgreSQL database if it doesn't exist
echo "Creating PostgreSQL database if it doesn't exist..."
psql -U postgres -h localhost -c "CREATE DATABASE pursuit_data;" || echo "Database already exists, continuing..."

# Run the Spark ETL pipeline using spark-submit
echo "Starting Spark ETL Pipeline..."
spark-submit \
  --master local[*] \
  --jars ~/spark/jars/postgresql-42.3.1.jar \
  --driver-memory 2g \
  spark_etl.py

# Check if the pipeline was successful
if [ $? -eq 0 ]; then
  echo "ETL Pipeline completed successfully!"
  
  echo "Here are some example queries you can run in PostgreSQL:"
  echo "
  -- Query 1: Find all entities where a contact has a title containing 'finance'
  SELECT DISTINCT 
      p.place_id, 
      p.display_name AS entity_name, 
      p.state_abbr, 
      p.pop_estimate_2022 AS population,
      string_agg(DISTINCT t.name, ', ') AS technologies
  FROM 
      places p
  JOIN 
      contacts c ON p.place_id = c.place_id
  LEFT JOIN 
      techstacks t ON p.place_id = t.place_id
  WHERE 
      c.title ILIKE '%finance%'
  GROUP BY 
      p.place_id, p.display_name, p.state_abbr, p.pop_estimate_2022
  ORDER BY 
      p.display_name;

  -- Query 2: Find contacts where email contains 'bob', entity has technology 'Accela', and population > 10k
  SELECT 
      c.contact_id,
      c.first_name,
      c.last_name,
      c.emails,
      c.phone,
      c.title,
      p.display_name AS entity_name,
      p.pop_estimate_2022 AS population
  FROM 
      contacts c
  JOIN 
      places p ON c.place_id = c.place_id
  JOIN 
      techstacks t ON p.place_id = t.place_id
  WHERE 
      c.emails ILIKE '%bob%'
      AND t.name = 'Accela'
      AND p.pop_estimate_2022 > 10000;

  -- Query 3: Find all entities synced with a specific customer's CRM
  SELECT 
      p.place_id,
      p.display_name AS entity_name,
      p.state_abbr,
      p.pop_estimate_2022 AS population,
      cm.external_crm_id,
      cm.account_owner,
      string_agg(DISTINCT t.name, ', ') AS technologies
  FROM 
      places p
  JOIN 
      customer_mappings cm ON p.place_id = cm.place_id
  LEFT JOIN 
      techstacks t ON p.place_id = t.place_id
  WHERE 
      cm.customer_id = 'CustomerA'
  GROUP BY 
      p.place_id, p.display_name, p.state_abbr, p.pop_estimate_2022, cm.external_crm_id, cm.account_owner;"
else
  echo "ETL Pipeline failed. Please check the logs for details."
  exit 1
fi

echo "==================== PIPELINE COMPLETE ===================="