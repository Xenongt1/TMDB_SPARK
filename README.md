# TMDB Movie Analytics

**Overview**
This project collects movie data from the TMDB API, prepares it for analysis, and explores performance patterns across budget, revenue, ratings, franchises, and popularity. The workflow follows a standard data pipeline that includes extraction, cleaning, transformation, KPI development, and visualization.

# Data Extraction

Movie information is retrieved directly from the TMDB API for a chosen list of movie IDs. Each API response includes details such as:

Titles, summaries, and release dates

Cast, crew, and directors

Genres, production companies, and countries

Budgets, revenues, popularity, and ratings

All responses are stored and compiled into a Pandas DataFrame.

# Data Cleaning and Transformation

The raw API output contains nested dictionaries, inconsistent formats, and missing values.
Key cleaning steps include:

Flattening list-based fields (genres, companies, countries, languages)

Converting budgets and revenues into numeric form and scaling them into millions

Creating new metrics such as profit and ROI

Handling invalid dates, zero budgets, and missing entries

Filtering out incomplete or irrelevant movie records

Reordering columns for clarity

This results in a structured dataset suitable for analysis.

# KPI Analysis

Several performance metrics are calculated to compare movies and identify trends. These include:

Highest revenue, budget, profit, and ROI

Lowest ROI and biggest financial losses

Most popular and most voted movies

Franchise-level statistics using belongs_to_collection

Director-level revenue and rating summaries

Custom search queries combining genres, cast, and directors

A reusable ranking function is used to generate consistent KPI tables.

# Visualizations

Matplotlib is used to explore relationships and historical patterns through:

Budget vs. revenue scatterplots

ROI distribution histograms

Popularity vs. rating scatterplots

Yearly total revenue trends

These charts help highlight financial patterns, rating behavior, and changes in the film industry over time.

