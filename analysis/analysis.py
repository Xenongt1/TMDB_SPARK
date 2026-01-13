
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def calculate_kpis(df: DataFrame) -> DataFrame:
    """
    Calculates Profit and ROI using Spark expressions.
    """
    if df is None:
        return None
        
    # Check if columns exist
    cols = df.columns
    if 'revenue_musd' in cols and 'budget_musd' in cols:
        df = df.withColumn('profit_musd', F.col('revenue_musd') - F.col('budget_musd'))
        
        # ROI: only compute if budget is present and > 0
        df = df.withColumn('roi', 
            F.when(
                (F.col('budget_musd').isNotNull()) & (F.col('budget_musd') > 0),
                F.col('revenue_musd') / F.col('budget_musd')
            ).otherwise(None)
        )
    return df

def rank_movies(
    df: DataFrame, 
    score_col: str, 
    top_n: int = 10, 
    ascending: bool = False,
    min_budget: float = None, 
    min_votes: int = None
) -> DataFrame:
    """
    Refined query to fetch top movies using Spark.
    Returns a new DataFrame with the top N results.
    """
    temp = df

    # Filters
    if min_budget is not None and 'budget_musd' in temp.columns:   
        temp = temp.filter(F.col('budget_musd') >= min_budget)
        
    if min_votes is not None and 'vote_count' in temp.columns:
        temp = temp.filter(F.col('vote_count') >= min_votes)
    
    if score_col not in temp.columns:
        print(f"Column {score_col} not found in DataFrame.")
        return None

    # Filter Not Null
    temp = temp.filter(F.col(score_col).isNotNull())
    
    # Sort and Limit
    sort_expr = F.col(score_col).asc() if ascending else F.col(score_col).desc()
    temp = temp.orderBy(sort_expr).limit(top_n)

    cols_to_select = ['title', 'release_date', 'budget_musd', 'revenue_musd',
            'profit_musd', 'roi', 'vote_count', 'vote_average', 'popularity']
            
    # Select available columns
    available_cols = [c for c in cols_to_select if c in temp.columns]
    return temp.select(*available_cols)

def get_franchise_performance(df: DataFrame) -> DataFrame:
    """
    Aggregates metrics by franchise (collection).
    """
    if 'belongs_to_collection' not in df.columns:
        return None
        
    # Filter where collection is not null
    franchises = df.filter(F.col('belongs_to_collection').isNotNull()) \
        .groupBy('belongs_to_collection') \
        .agg(
            F.count('title').alias('movie_count'),
            F.sum('budget_musd').alias('total_budget'),
            F.mean('budget_musd').alias('avg_budget'),
            F.sum('revenue_musd').alias('total_revenue'),
            F.mean('revenue_musd').alias('avg_revenue'),
            F.mean('vote_average').alias('avg_rating'),
            F.mean('popularity').alias('avg_popularity')
        )
    
    # Sort by total revenue descending
    return franchises.orderBy(F.col('total_revenue').desc())

def get_director_performance(df: DataFrame) -> DataFrame:
    """
    Aggregates metrics by director.
    """
    if 'director' not in df.columns:
        return None
        
    directors = df.groupBy('director').agg(
        F.count('title').alias('movies_directed'),
        F.sum('budget_musd').alias('total_budget'),
        F.sum('revenue_musd').alias('total_revenue'),
        F.mean('vote_average').alias('avg_rating')
    )
    
    return directors.orderBy(F.col('total_revenue').desc())
