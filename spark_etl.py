from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim, lit, regexp_replace, when
import os
import logging
import time
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"spark_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("spark_etl")

# Configuration
DB_CONFIG = {
    "url": "jdbc:postgresql://localhost:5432/pursuit_data",
    "user": "postgres",
    "password": "Spoorthi@1820",
    "driver": "org.postgresql.Driver"
}

# File paths
FILE_PATHS = {
    "contacts": "contacts.csv",
    "places": "places.csv",
    "techstacks": "techstacks.csv",
    "customer_mappings": "customer_mappings.csv",
    "contact_sync_status": "contact_sync_status.csv"
}

class SparkETL:
    """Main ETL class that uses Spark to process data and load it into PostgreSQL"""
    
    def __init__(self, db_config, file_paths):
        """Initialize with database config and file paths"""
        self.db_config = db_config
        self.file_paths = file_paths
        self.spark = None
        self.start_time = None
    
    def init_spark(self):
        """Initialize the Spark session"""
        logger.info("Initializing Spark session")
        
        self.spark = (SparkSession.builder
            .appName("Contact-Entity-Pipeline")
            .config("spark.jars", "/Users/spoorthiv/spark/jars/postgresql-42.3.1.jar")  # Path to PostgreSQL JDBC driver
            .config("spark.sql.adaptive.enabled", "true")  # Enable adaptive query execution
            .config("spark.sql.shuffle.partitions", "10")  # Adjust based on your data size
            .getOrCreate())
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
    
    def load_csv(self, file_path):
        """Load CSV file with Spark"""
        logger.info(f"Loading data from {file_path}")
        
        try:
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"File not found: {file_path}")
                
            df = (self.spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .option("mode", "DROPMALFORMED")  # Skip malformed records
                 .csv(file_path))
            
            count = df.count()
            logger.info(f"Successfully loaded {count} records from {file_path}")
            return df
            
        except Exception as e:
            logger.error(f"Error loading {file_path}: {str(e)}")
            raise
    
    def clean_places(self, df):
        """Clean places/entities data using Spark transformations"""
        logger.info("Cleaning places data")
        
        # Convert numeric columns to proper types
        if "pop_estimate_2022" in df.columns:
            df = df.withColumn("pop_estimate_2022", col("pop_estimate_2022").cast("float"))
        
        if "lat" in df.columns:
            df = df.withColumn("lat", col("lat").cast("float"))
            
        if "long" in df.columns:
            df = df.withColumn("long", col("long").cast("float"))
        
        if "place_fips" in df.columns:
            df = df.withColumn("place_fips", col("place_fips").cast("float"))
            
        if "lsadc" in df.columns:
            df = df.withColumn("lsadc", col("lsadc").cast("float"))
        
        if "sum_lev" in df.columns:
            df = df.withColumn("sum_lev", col("sum_lev").cast("integer"))
        
        # Handle missing values
        if "display_name" in df.columns:
            df = df.withColumn("display_name",
                              when(col("display_name").isNull() | (col("display_name") == ""), "Unknown Entity")
                              .otherwise(col("display_name")))
        
        # Clean URL field
        if "url" in df.columns:
            df = df.withColumn("url", trim(col("url")))
        
        # Add timestamps if they don't exist
        if "created_at" not in df.columns:
            df = df.withColumn("created_at", lit(datetime.now()))
            
        if "updated_at" not in df.columns:
            df = df.withColumn("updated_at", lit(datetime.now()))
        
        logger.info("Places data cleaning complete")
        return df
    
    def clean_contacts(self, df):
        """Clean contacts data using Spark transformations"""
        logger.info("Cleaning contacts data")
        
        # Normalize email (lowercase)
        if "emails" in df.columns:
            df = df.withColumn("emails", 
                              when(col("emails").isNotNull(), lower(trim(col("emails"))))
                              .otherwise(None))
        
        # Normalize phone numbers
        if "phone" in df.columns:
            df = df.withColumn("phone", 
                              when(col("phone").isNotNull(), 
                                  regexp_replace(col("phone"), "[^0-9+]", ""))
                              .otherwise(None))
        
        # Clean URL field
        if "url" in df.columns:
            df = df.withColumn("url", trim(col("url")))
        
        # Handle missing values for string columns
        str_cols = ["first_name", "last_name", "title", "department"]
        for column in str_cols:
            if column in df.columns:
                df = df.withColumn(column, 
                                  when(col(column).isNull() | (col(column) == ""), "")
                                  .otherwise(col(column)))
        
        # Parse created_at if it's a string with JS date format
        if "created_at" in df.columns:
            # This is just a placeholder - you might need more complex parsing logic
            # depending on your actual date format
            df = df.withColumn("created_at", col("created_at").cast("timestamp"))
        else:
            df = df.withColumn("created_at", lit(datetime.now()))
            
        if "updated_at" not in df.columns:
            df = df.withColumn("updated_at", lit(datetime.now()))
        
        # Filter out invalid contacts (no email and no phone)
        if "emails" in df.columns and "phone" in df.columns:
            df = df.filter(
                (col("emails").isNotNull() & (col("emails") != "")) | 
                (col("phone").isNotNull() & (col("phone") != ""))
            )
        
        logger.info("Contacts data cleaning complete")
        return df
    
    def clean_techstacks(self, df):
        """Clean techstacks data using Spark transformations"""
        logger.info("Cleaning techstacks data")
        
        # Normalize technology name
        if "name" in df.columns:
            df = df.withColumn("name", trim(col("name")))
            
            # Filter out rows with missing technology name
            df = df.filter(col("name").isNotNull() & (col("name") != ""))
        
        # Add timestamp if it doesn't exist
        if "created_at" not in df.columns:
            df = df.withColumn("created_at", lit(datetime.now()))
        
        logger.info("Techstacks data cleaning complete")
        return df
    
    def clean_customer_mappings(self, df):
        """Clean customer mappings data using Spark transformations"""
        logger.info("Cleaning customer mappings data")
        
        # Filter out rows with missing customer_id or external_crm_id
        if "customer_id" in df.columns and "external_crm_id" in df.columns:
            df = df.filter(
                col("customer_id").isNotNull() & (col("customer_id") != "") &
                col("external_crm_id").isNotNull() & (col("external_crm_id") != "")
            )
        
        # Add timestamp if it doesn't exist
        if "created_at" not in df.columns:
            df = df.withColumn("created_at", lit(datetime.now()))
        
        logger.info("Customer mappings data cleaning complete")
        return df
    
    def clean_contact_sync_status(self, df):
        """Clean contact sync status data using Spark transformations"""
        logger.info("Cleaning contact sync status data")
        
        # Log initial count
        initial_count = df.count()
        logger.info(f"Contact sync records before cleaning: {initial_count}")
        
        # Convert contact_id to string to match contacts table
        if "contact_id" in df.columns:
            df = df.withColumn("contact_id", col("contact_id").cast("string"))
        
        # Only filter out rows with completely missing IDs
        if "contact_id" in df.columns and "customer_id" in df.columns:
            df = df.filter(
                col("contact_id").isNotNull() & 
                col("customer_id").isNotNull()
            )
        
        # Convert last_synced_at to timestamp
        if "last_synced_at" in df.columns:
            df = df.withColumn("last_synced_at", col("last_synced_at").cast("timestamp"))
        
        # Fill missing status with 'unknown'
        if "synced_status" in df.columns:
            df = df.withColumn("synced_status", 
                              when(col("synced_status").isNull(), "unknown")
                              .otherwise(col("synced_status")))
        
        # Log final count
        final_count = df.count()
        logger.info(f"Contact sync records after cleaning: {final_count}")
        logger.info(f"Removed {initial_count - final_count} invalid records")
        
        logger.info("Contact sync status data cleaning complete")
        return df
    
    def write_to_postgres(self, df, table_name, mode="append"):
        """Write DataFrame to PostgreSQL"""
        logger.info(f"Writing {df.count()} records to {table_name}")
        
        try:
            # Show sample data
            logger.info(f"Sample data for {table_name}:")
            df.show(5, truncate=False)
            
            # Write to PostgreSQL
            df.write \
                .format("jdbc") \
                .option("url", self.db_config["url"]) \
                .option("dbtable", table_name) \
                .option("user", self.db_config["user"]) \
                .option("password", self.db_config["password"]) \
                .option("driver", self.db_config["driver"]) \
                .mode(mode) \
                .save()
            
            logger.info(f"Successfully wrote data to {table_name}")
            
        except Exception as e:
            logger.error(f"Error writing to {table_name}: {str(e)}")
            raise
    
    def create_postgres_schema(self):
        """
        Create PostgreSQL schema directly using JDBC connection
        Note: In a production environment, you might want to separate this
        into a separate script or use a migration tool like Flyway or Liquibase
        """
        logger.info("Creating PostgreSQL schema")
        
        # We'll use Spark's JDBC connection to execute SQL statements
        # This is a bit of a hack but works for demonstration purposes
        
        # Define SQL statements to create tables and indexes
        create_places_table = """
        CREATE TABLE IF NOT EXISTS places (
            place_id VARCHAR(255) PRIMARY KEY,
            state_abbr VARCHAR(2),
            lat FLOAT,
            long FLOAT,
            pop_estimate_2022 FLOAT,
            place_fips FLOAT,
            sum_lev INTEGER,
            url VARCHAR(255),
            lsadc FLOAT,
            display_name VARCHAR(255),
            parent_id VARCHAR(255),
            address VARCHAR(255),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
        
        create_contacts_table = """
        CREATE TABLE IF NOT EXISTS contacts (
            contact_id VARCHAR(255) PRIMARY KEY,
            place_id VARCHAR(255) REFERENCES places(place_id),
            first_name VARCHAR(255),
            last_name VARCHAR(255),
            emails VARCHAR(255),
            phone VARCHAR(255),
            url VARCHAR(255),
            title VARCHAR(255),
            department VARCHAR(255),
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
        
        create_techstacks_table = """
        CREATE TABLE IF NOT EXISTS techstacks (
            tech_id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            place_id VARCHAR(255) REFERENCES places(place_id),
            type VARCHAR(255),
            created_at TIMESTAMP
        )
        """
        
        create_customer_mappings_table = """
        CREATE TABLE IF NOT EXISTS customer_mappings (
            mapping_id INTEGER PRIMARY KEY,
            customer_id VARCHAR(255) NOT NULL,
            external_crm_id VARCHAR(255) NOT NULL,
            place_id VARCHAR(255) REFERENCES places(place_id),
            account_owner VARCHAR(255),
            created_at TIMESTAMP
        )
        """
        
        # Modified: Removed foreign key constraint to allow loading all sync records
        create_contact_sync_table = """
        CREATE TABLE IF NOT EXISTS contact_sync_status (
            contact_id VARCHAR(255),
            customer_id VARCHAR(255) NOT NULL,
            synced_status VARCHAR(50),
            last_synced_at TIMESTAMP,
            PRIMARY KEY (contact_id, customer_id)
        )
        """
        
        # Execute the SQL statements
        statements = [
            create_places_table,
            create_contacts_table,
            create_techstacks_table,
            create_customer_mappings_table,
            create_contact_sync_table
        ]
        
        # Using a temporary DataFrame to execute SQL through JDBC
        temp_df = self.spark.createDataFrame([("1",)], ["dummy"])
        
        for stmt in statements:
            try:
                # We use a dummy select query that never executes, but allows us to establish
                # a JDBC connection through which we can execute our DDL statements
                self.spark.read.format("jdbc") \
                    .option("url", self.db_config["url"]) \
                    .option("user", self.db_config["user"]) \
                    .option("password", self.db_config["password"]) \
                    .option("driver", self.db_config["driver"]) \
                    .option("dbtable", f"({stmt}; SELECT 1 as dummy WHERE false) as tmp") \
                    .load()
                
                logger.info(f"Successfully executed: {stmt}")
            except Exception as e:
                logger.error(f"Error executing statement: {e}")
                # Continue to next statement rather than failing completely
        
        logger.info("PostgreSQL schema creation complete")
    
    def execute_pipeline(self):
        """Execute the complete ETL pipeline"""
        self.start_time = time.time()
        logger.info("Starting ETL pipeline execution")
        
        try:
            # Initialize Spark
            self.init_spark()
            
            # Create database schema
            self.create_postgres_schema()
            
            # Process places data first (for foreign key constraints)
            places_df = self.load_csv(self.file_paths["places"])
            places_df = self.clean_places(places_df)
            self.write_to_postgres(places_df, "places")
            
            # Process techstacks data
            techstacks_df = self.load_csv(self.file_paths["techstacks"])
            techstacks_df = self.clean_techstacks(techstacks_df)
            self.write_to_postgres(techstacks_df, "techstacks")
            
            # Process customer mappings data
            mappings_df = self.load_csv(self.file_paths["customer_mappings"])
            mappings_df = self.clean_customer_mappings(mappings_df)
            self.write_to_postgres(mappings_df, "customer_mappings")
            
            # Process contacts data
            contacts_df = self.load_csv(self.file_paths["contacts"])
            contacts_df = self.clean_contacts(contacts_df)
            self.write_to_postgres(contacts_df, "contacts")
            
            # Process contact sync status data
            sync_df = self.load_csv(self.file_paths["contact_sync_status"])
            sync_df = self.clean_contact_sync_status(sync_df)
            self.write_to_postgres(sync_df, "contact_sync_status")
            
            # Calculate total execution time
            end_time = time.time()
            execution_time = end_time - self.start_time
            
            logger.info(f"ETL pipeline executed successfully in {execution_time:.2f} seconds")
            
            # Print summary statistics
            logger.info("\nPipeline Execution Summary:")
            logger.info(f"Places processed: {places_df.count()}")
            logger.info(f"Contacts processed: {contacts_df.count()}")
            logger.info(f"Techstacks processed: {techstacks_df.count()}")
            logger.info(f"Customer mappings processed: {mappings_df.count()}")
            logger.info(f"Contact sync records processed: {sync_df.count()}")
            logger.info(f"Total execution time: {execution_time:.2f} seconds")
            
            return True
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {str(e)}")
            return False
        finally:
            # Stop Spark session
            if self.spark:
                self.spark.stop()
                logger.info("Spark session stopped")

if __name__ == "__main__":
    # Execute the pipeline
    etl = SparkETL(DB_CONFIG, FILE_PATHS)
    success = etl.execute_pipeline()
    
    if success:
        print("ETL Pipeline completed successfully! Data is now available in PostgreSQL.")
        print("You can now run your example queries directly against the database.")
    else:
        print("ETL Pipeline failed. Please check the logs for details.")