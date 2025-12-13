
import pandas as pd
import numpy as np

def calculate_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates Profit and ROI.
    """
    if df.empty:
        return df
        
    if 'revenue_musd' in df.columns and 'budget_musd' in df.columns:
        df['profit_musd'] = df['revenue_musd'] - df['budget_musd']
        
        # ROI: only compute if budget is present and > 0
        df['roi'] = np.where(
            (df['budget_musd'].notna()) & (df['budget_musd'] > 0),
            df['revenue_musd'] / df['budget_musd'],
            np.nan
        )
    return df

def rank_movies(
    df: pd.DataFrame, 
    score_col: str, 
    top_n: int = 10, 
    ascending: bool = False,
    min_budget: float = None, 
    min_votes: int = None
) -> pd.DataFrame:
    """
    General purpose ranking function.
    """
    temp = df.copy()

    if min_budget is not None and 'budget_musd' in temp.columns:   
        temp = temp[temp['budget_musd'].ge(min_budget)]
        
    if min_votes is not None and 'vote_count' in temp.columns:
        temp = temp[temp['vote_count'].ge(min_votes)]
    
    if score_col not in temp.columns:
        print(f"Column {score_col} not found in DataFrame.")
        return pd.DataFrame()

    temp = temp[temp[score_col].notna()]
    temp = temp.sort_values(score_col, ascending=ascending).head(top_n)

    cols = ['title', 'release_date', 'budget_musd', 'revenue_musd',
            'profit_musd', 'roi', 'vote_count', 'vote_average', 'popularity']
    # Return only available columns
    cols = [c for c in cols if c in temp.columns]

    return temp[cols]

def get_franchise_performance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates metrics by franchise (collection).
    """
    if 'belongs_to_collection' not in df.columns:
        return pd.DataFrame()
        
    franchises = df.groupby('belongs_to_collection').agg({
        'title': 'count',
        'budget_musd': ['sum', 'mean'],
        'revenue_musd': ['sum', 'mean'],
        'vote_average': 'mean',
        'popularity': 'mean'
    })
    
    # Flatten Hierarchical Index
    franchises.columns = [
        'movie_count', 'total_budget', 'avg_budget', 
        'total_revenue', 'avg_revenue', 'avg_rating', 'avg_popularity'
    ]
    
    # Sort by total revenue descending
    return franchises.sort_values('total_revenue', ascending=False)

def get_director_performance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates metrics by director.
    """
    if 'director' not in df.columns:
        return pd.DataFrame()
        
    directors = df.groupby('director').agg({
        'title': 'count',
        'budget_musd': 'sum',
        'revenue_musd': 'sum',
        'vote_average': 'mean'
    })
    
    directors.columns = ['movies_directed', 'total_budget', 'total_revenue', 'avg_rating']
    return directors.sort_values('total_revenue', ascending=False)

def filter_bruce_willis_scifi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Search 1: Find the best-rated Science Fiction Action movies starring Bruce Willis.
    Sorted by Rating (highest to lowest).
    """
    if df.empty or 'genres' not in df.columns or 'cast' not in df.columns:
        return pd.DataFrame()

    mask = (
        df['genres'].astype(str).str.contains("Science Fiction", case=False) &
        df['genres'].astype(str).str.contains("Action", case=False) &
        df['cast'].astype(str).str.contains("Bruce Willis", case=False)
    )
    
    filtered = df[mask].sort_values('vote_average', ascending=False)
    return filtered[['title', 'vote_average', 'genres', 'release_date']]

def filter_uma_tarantino(df: pd.DataFrame) -> pd.DataFrame:
    """
    Search 2: Find movies starring Uma Thurman, directed by Quentin Tarantino.
    Sorted by runtime (shortest to longest).
    """
    if df.empty:
        return pd.DataFrame()
        
    mask = pd.Series([True] * len(df))
    
    if 'cast' in df.columns:
        mask &= df['cast'].astype(str).str.contains("Uma Thurman", case=False)
    if 'director' in df.columns:
        mask &= df['director'].astype(str).str.contains("Quentin Tarantino", case=False)
        
    filtered = df[mask]
    if 'runtime' in filtered.columns:
        filtered = filtered.sort_values('runtime', ascending=True)
        
    return filtered[['title', 'director', 'runtime', 'release_date']]

def compare_franchise_vs_standalone(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare attributes of Franchise movies vs Standalone movies.
    """
    if 'belongs_to_collection' not in df.columns:
        return pd.DataFrame()

    df['is_franchise'] = df['belongs_to_collection'].notna()
    
    comp = df.groupby('is_franchise').agg({
        'revenue_musd': 'mean',
        'budget_musd': 'mean',
        'popularity': 'mean',
        'vote_average': 'mean',
        'roi': 'median'
    }).rename(index={True: "Franchise", False: "Standalone"})
    
    return comp
