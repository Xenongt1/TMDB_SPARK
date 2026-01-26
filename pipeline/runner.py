import os
import json
from pyspark.sql import SparkSession
from config.settings import MOVIE_IDS
from extraction.api import fetch_movie_data
from transformation.cleaning import clean_movie_data
from analysis.analysis import (
    calculate_kpis, rank_movies, get_franchise_performance, get_director_performance
)
from visualization.plots import (
    set_style, plot_revenue_vs_budget, plot_roi_by_genre, 
    plot_popularity_vs_rating, plot_franchise_comparison, plot_yearly_trends
)
from config.logger import setup_logger

logger = setup_logger()

def main():
    logger.info("=== TMDB Data Pipeline Started (PySpark) ===")
    
    # Initialize Spark Session
    # Using local[*] to run on all available cores
    spark = SparkSession.builder \
        .appName("TMDB_Pipeline") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # Setup directories
    os.makedirs('outputs/data', exist_ok=True)
    os.makedirs('outputs/plots', exist_ok=True)
    
    # 1. Fetch Data
    logger.info("--- Step 1: Fetching Data ---")
    raw_data_list = fetch_movie_data(MOVIE_IDS)
    if not raw_data_list:
        logger.error("No data fetched. Exiting.")
        return
        
    # Create Spark DataFrame from list of dicts
    logger.info("Creating Spark DataFrame via JSON RDD for robustness...")
    # Convert list of dicts to RDD of JSON strings
    json_rdd = spark.sparkContext.parallelize([json.dumps(r) for r in raw_data_list])
    # Read JSON RDD (Spark automatically infers complex nested schema)
    raw_df = spark.read.json(json_rdd)
    
    # Save raw data as Parquet (Standard for Spark pipelines)
    try:
        raw_df.write.mode("overwrite").parquet('outputs/data/raw_movies.parquet')
        logger.info("Raw data saved to outputs/data/raw_movies.parquet")
    except Exception as e:
        logger.error(f"Failed to save raw parquet: {e}")

    # 2. Clean Data
    logger.info("--- Step 2: Cleaning Data ---")
    cleaned_df = clean_movie_data(raw_df)
    
    # Cache cleaned data as it's used multiple times
    cleaned_df.cache()
    
    # Save cleaned data as Parquet
    try:
        cleaned_df.write.mode("overwrite").parquet('outputs/data/cleaned_movies.parquet')
        logger.info("Cleaned data saved to outputs/data/cleaned_movies.parquet")
        
        # Save a human-readable CSV preview (first 100 rows)
        # We need to flatten array columns (cast, genres, etc.) because CSV doesn't support complex types
        from pyspark.sql.functions import col, concat_ws
        
        # Identify array columns
        array_cols = [f.name for f in cleaned_df.schema.fields if "ArrayType" in str(f.dataType)]
        
        preview_df = cleaned_df.limit(100)
        for c in array_cols:
            preview_df = preview_df.withColumn(c, concat_ws(", ", col(c)))
            
        preview_df.coalesce(1).write.mode("overwrite").option("header", "true").csv('outputs/data/cleaned_movies_preview.csv')
        logger.info("Human-readable preview saved to outputs/data/cleaned_movies_preview.csv")
    except Exception as e:
        logger.error(f"Failed to save cleaned data exports: {e}")

    # 3. Analyze & KPIs
    logger.info("--- Step 3: Analysis & KPIs ---")
    df = calculate_kpis(cleaned_df)
    
    # Top 5 Revenue
    top_revenue = rank_movies(df, 'revenue_musd', top_n=5, ascending=False)
    if top_revenue:
        print("\nTop 5 Movies by Revenue:")
        top_revenue.show(truncate=False)

    # Top 5 ROI (Budget >= 10M)
    top_roi = rank_movies(df, 'roi', top_n=5, ascending=False, min_budget=10)
    if top_roi:
        print("\nTop 5 Movies by ROI (Budget >= 10M):")
        top_roi.show(truncate=False)
    
    # Franchise Performance
    franchises = get_franchise_performance(df)
    if franchises:
        print("\nTop Franchises by Revenue:")
        franchises.select('belongs_to_collection', 'total_revenue', 'avg_rating').show(5, truncate=False)

    # Director Performance
    directors = get_director_performance(df)
    if directors:
        print("\nTop Directors by Revenue:")
        directors.select('director', 'total_revenue', 'avg_rating').show(5, truncate=False)

    # 4. Visualization
    logger.info("--- Step 4: Visualization ---")
    # Visualization libraries only work with Pandas. 
    # We collect the necessary data to the driver.
    
    # Collect main dataset for plotting
    df_pd = df.toPandas()
    
    set_style()
    
    plot_revenue_vs_budget(df_pd, save_path='outputs/plots/revenue_vs_budget.png', show=False)
    plot_roi_by_genre(df_pd, save_path='outputs/plots/roi_by_genre.png', show=False)
    plot_popularity_vs_rating(df_pd, save_path='outputs/plots/popularity_vs_rating.png', show=False)
    plot_yearly_trends(df_pd, save_path='outputs/plots/yearly_trends.png', show=False)
    
    if franchises:
        # Collect franchise data for plotting
        franchises_pd = franchises.toPandas()
        # Rename index to match what the plot function expects if it relied on index
        # The plot function uses columns: 'total_revenue', 'avg_rating'. 
        # But wait, original code expected the franchise name to be the index.
        # Our Spark aggregation put 'belongs_to_collection' as a column.
        franchises_pd.set_index('belongs_to_collection', inplace=True)
        
        plot_franchise_comparison(franchises_pd, save_path='outputs/plots/franchise_comparison.png', show=False)

    logger.info("=== Pipeline Completed Successfully ===")
    
    spark.stop()

if __name__ == "__main__":
    main()
