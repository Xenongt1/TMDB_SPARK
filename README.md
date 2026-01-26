# TMDB Movie Data Pipeline (PySpark)

## Overview
This project processes TMDB movie data using a **distributed PySpark pipeline**. It demonstrates a scalable ETL (Extraction, Transformation, Load/Analysis) workflow, calculating key performance indicators (KPIs) and generating visualizations for movie revenue, ROI, and franchise performance using **native Spark SQL functions** instead of Pandas.

## Features
- **Distributed Processing**: Powered by PySpark for scalability.
- **Native Spark Functions**: Uses `pyspark.sql.functions` for high-performance data transformation (avoids Python UDFs).
- **Modular Architecture**: Code is organized into `extraction`, `transformation`, `analysis`, and `visualization` packages.
- **Rich Outputs**:
    - **Data**: Saved as compressed **Parquet files** for Spark processing, with **CSV previews** for human inspection (`outputs/data/`).
    - **Plots**: Saved as high-quality PNG images (`outputs/plots/`).

## Project Structure
```
.
├── extraction/         # API Request Logic
│   └── api.py
├── transformation/     # Data Cleaning & Parsing (Spark)
│   └── cleaning.py
├── analysis/           # KPIs, Rankings, & Comparisons (Spark)
│   └── analysis.py
├── visualization/      # Plotting Functions (Pandas Interface)
│   └── plots.py
├── pipeline/           # Orchestration (SparkSession)
│   └── runner.py
├── config/             # Settings & Keys
│   └── settings.py
├── notebooks/          # Presentation Layer
│   └── final_report.ipynb
├── outputs/            # Generated Results
│   ├── data/
│   └── plots/
├── .env                # API Keys (GitIgnored)
└── requirements.txt    # Dependencies
```

## Setup & Installation

1.  **Prerequisites**:
    - Python 3.8+
    - **Java JDK (8, 11, or 17)** is required for PySpark.
    - Git

2.  **Clone the Repository**:
    ```bash
    git clone https://github.com/Xenongt1/TMDB_SPARK.git
    cd TMDB_SPARK
    ```

3.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure API Key**:
    - Create a `.env` file in the root directory (copy `.env.example` if available, or just create one).
    - Add your TMDB API Key:
      ```
      TMDB_API_KEY=your_api_key_here
      ```

## Usage

### Run the Pipeline
To execute the full ETL process using Spark:
```bash
python3 -m pipeline.runner
```
*Results will be saved to `outputs/`.*

## Outputs
- **`outputs/data/`**:
    - `cleaned_movies.parquet/`: The fully processed dataset in optimized binary format.
    - `cleaned_movies_preview.csv/`: A human-readable snippet (top 100 rows) for quick inspection.
- **`outputs/plots/`**:
    - `revenue_vs_budget.png`
    - `roi_by_genre.png`
    - `popularity_vs_rating.png`
    - `yearly_trends.png`
    - `franchise_comparison.png`
