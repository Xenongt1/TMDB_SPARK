
import os
import shutil
import pandas as pd
from src.config import MOVIE_IDS
from src.api import fetch_movie_data
from src.cleaning import clean_movie_data
from src.analysis import (
    calculate_kpis, rank_movies, get_franchise_performance, get_director_performance
)
from src.visualization import (
    set_style, plot_revenue_vs_budget, plot_roi_by_genre, 
    plot_popularity_vs_rating, plot_franchise_comparison, plot_yearly_trends
)

def main():
    print("=== TMDB Data Pipeline Started ===")
    
    # Setup directories
    os.makedirs('outputs/data', exist_ok=True)
    os.makedirs('outputs/plots', exist_ok=True)
    
    # 1. Fetch Data
    print("\n--- Step 1: Fetching Data ---")
    raw_df = fetch_movie_data(MOVIE_IDS)
    if raw_df.empty:
        print("No data fetched. Exiting.")
        return
    
    # Save raw data (HTML)
    raw_df.to_html('outputs/data/raw_movies.html', index=False)
    print("Raw data saved to outputs/data/raw_movies.html")

    # 2. Clean Data
    print("\n--- Step 2: Cleaning Data ---")
    cleaned_df = clean_movie_data(raw_df)
    
    # Save cleaned data (HTML)
    cleaned_df.to_html('outputs/data/cleaned_movies.html', index=False)
    print("Cleaned data saved to outputs/data/cleaned_movies.html")

    # 3. Analyze & KPIs
    print("\n--- Step 3: Analysis & KPIs ---")
    df = calculate_kpis(cleaned_df)
    
    # Top 5 Revenue
    top_revenue = rank_movies(df, 'revenue_musd', top_n=5, ascending=False)
    print("\nTop 5 Movies by Revenue:")
    print(top_revenue[['title', 'revenue_musd']])

    # Top 5 ROI (Budget >= 10M)
    top_roi = rank_movies(df, 'roi', top_n=5, ascending=False, min_budget=10)
    print("\nTop 5 Movies by ROI (Budget >= 10M):")
    print(top_roi[['title', 'roi']])
    
    # Franchise Performance
    franchises = get_franchise_performance(df)
    if not franchises.empty:
        print("\nTop Franchises by Revenue:")
        print(franchises[['total_revenue', 'avg_rating']].head())

    # Director Performance
    directors = get_director_performance(df)
    if not directors.empty:
        print("\nTop Directors by Revenue:")
        print(directors[['total_revenue', 'avg_rating']].head())

    # 4. Visualization
    print("\n--- Step 4: Visualization ---")
    set_style()
    
    plot_revenue_vs_budget(df, save_path='outputs/plots/revenue_vs_budget.png', show=False)
    plot_roi_by_genre(df, save_path='outputs/plots/roi_by_genre.png', show=False)
    plot_popularity_vs_rating(df, save_path='outputs/plots/popularity_vs_rating.png', show=False)
    plot_yearly_trends(df, save_path='outputs/plots/yearly_trends.png', show=False)
    plot_franchise_comparison(franchises, save_path='outputs/plots/franchise_comparison.png', show=False)

    print("\n=== Pipeline Completed Successfully ===")
    print("Outputs saved to outputs/data/ and outputs/plots/")

if __name__ == "__main__":
    main()
