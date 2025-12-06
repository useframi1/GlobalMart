"""
Product Dimension ETL with SCD Type 2
Tracks historical changes to product attributes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, current_timestamp
from datetime import datetime
import sys
import os

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from batch_processing.etl.base_etl import DimensionETL
from batch_processing.warehouse.scd_manager import SCDType2Manager


class ProductDimensionETL(DimensionETL):
    """ETL for product dimension with SCD Type 2 support"""

    def __init__(self, spark):
        super().__init__(spark, "dim_products", use_scd2=True)
        self.scd_manager = SCDType2Manager(spark)

    def extract(self) -> DataFrame:
        """
        Extract product data
        In a real scenario, this would come from a product catalog or real-time aggregations
        """
        self.logger.info("Extracting product data")

        try:
            # Extract product information from trending_products in real-time DB
            query = """
                (SELECT DISTINCT
                    product_id,
                    product_name,
                    category,
                    AVG(product_price) as base_price
                FROM trending_products
                GROUP BY product_id, product_name, category) as product_data
            """

            df = (self.spark.read
                  .format("jdbc")
                  .option("url", self.realtime_jdbc_url)
                  .option("dbtable", query)
                  .option("user", self.realtime_jdbc_props["user"])
                  .option("password", self.realtime_jdbc_props["password"])
                  .option("driver", self.realtime_jdbc_props["driver"])
                  .load())

            self.stats["rows_processed"] = df.count()
            self.logger.info(f"Extracted {self.stats['rows_processed']} product records")

            return df

        except Exception as e:
            self.logger.warning(f"Could not extract from trending_products: {e}")
            self.logger.info("Creating empty product dimension")
            # Return empty DataFrame with correct schema
            return self.spark.createDataFrame([], """
                product_id STRING,
                product_name STRING,
                category STRING,
                base_price DOUBLE
            """)

    def transform(self, df: DataFrame) -> DataFrame:
        """
        Transform product data
        Add derived attributes like subcategory, cost, margin
        """
        self.logger.info("Transforming product dimension")

        if df.count() == 0:
            self.logger.info("No products to transform")
            return df

        # Add derived attributes
        # In production, subcategory would come from product catalog
        # Cost and margin calculations would be more sophisticated
        transformed = (df
            .withColumn("subcategory", lit(None).cast("string"))  # Would come from catalog
            .withColumn("cost", col("base_price") * 0.6)  # Simplified: assume 40% margin
            .withColumn("margin", col("base_price") * 0.4)
            .withColumn("is_active", lit(True))
            .withColumn("created_at", current_timestamp())
            .withColumn("updated_at", current_timestamp())
        )

        return transformed

    def load(self, df: DataFrame) -> None:
        """
        Load product dimension with SCD Type 2 logic
        """
        self.logger.info("Loading product dimension with SCD Type 2")

        if df.count() == 0:
            self.logger.info("No products to load")
            return

        # Columns to track for changes
        tracked_columns = [
            "product_name", "category", "subcategory",
            "base_price", "cost", "margin", "is_active"
        ]

        # Use SCD Type 2 manager to handle merge
        stats = self.scd_manager.merge_scd_type2(
            source_df=df,
            target_table="dim_products",
            jdbc_url=self.warehouse_jdbc_url,
            jdbc_properties=self.warehouse_jdbc_props,
            business_key="product_id",
            tracked_columns=tracked_columns,
            surrogate_key="product_sk"
        )

        # Update statistics
        self.stats["rows_inserted"] = stats["inserts"]
        self.stats["rows_updated"] = stats["updates"]

        self.logger.info(f"Product dimension loaded: {stats}")
