
import pandas as pd
import numpy as np
import ast
from typing import Any, List, Optional

def extract_names(cell: Any) -> Any:
    """
    Extracts 'name' values from a list of dictionaries and joins them with ' | '.
    """
    if isinstance(cell, list):
        return " | ".join([item.get('name', '') for item in cell if isinstance(item, dict)])
    return np.nan

def safe_parse(x: Any) -> Any:
    """
    Safely parses a string representation of a Python literal (e.g. dict or list).
    """
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        try:
            return ast.literal_eval(x)
        except (ValueError, SyntaxError):
            return {}
    return {}

def extract_cast(credits: Any) -> List[str]:
    """Extracts cast names from credits dictionary."""
    try:
        cast_list = credits.get("cast", [])
        return [c.get("name") for c in cast_list if "name" in c]
    except AttributeError:
        return []

def extract_director(credits: Any) -> Optional[str]:
    """Extracts director name from credits dictionary."""
    try:
        crew_list = credits.get("crew", [])
        for c in crew_list:
            if c.get("job") == "Director":
                return c.get("name")
    except AttributeError:
        pass
    return None

def extract_crew_size(credits: Any) -> int:
    """Extracts total crew size."""
    try:
        return len(credits.get("crew", []))
    except AttributeError:
        return 0

def clean_movie_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main function to clean and transform the raw movie DataFrame.
    """
    if df.empty:
        print("Empty DataFrame provided to cleaning function.")
        return df

    # 1. Drop irrelevant columns
    columns_to_drop = [
        'adult', 'backdrop_path', 'homepage', 'imdb_id', 
        'original_title', 'video', 'poster_path'
    ]
    df = df.drop(columns=columns_to_drop, errors='ignore')

    # 2. Flatten JSON-like columns
    json_columns = ['genres', 'production_countries', 'production_companies', 'spoken_languages']
    for col in json_columns:
        if col in df.columns:
            df[col] = df[col].apply(extract_names)

    # Special handling for belongs_to_collection
    if 'belongs_to_collection' in df.columns:
        df['belongs_to_collection'] = df['belongs_to_collection'].apply(
            lambda x: x.get('name') if isinstance(x, dict) else np.nan
        )

    # 3. Convert Data Types
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    
    number_cols = ['id', 'budget', 'revenue', 'popularity', 'vote_count', 'vote_average', 'runtime']
    for col in number_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 4. Handle Incorrect or Missing Values
    # Replace 0 budget/revenue/runtime with NaN
    for col in ['budget', 'revenue', 'runtime']:
        if col in df.columns:
            df.loc[df[col] == 0, col] = np.nan
            
    if 'vote_count' in df.columns:
        df.loc[df['vote_count'] == 0, 'vote_average'] = np.nan

    # Convert budget/revenue to Millions
    if 'budget' in df.columns:
        df['budget_musd'] = df['budget'] / 1_000_000
    if 'revenue' in df.columns:
        df['revenue_musd'] = df['revenue'] / 1_000_000

    # Clean text fields
    for col in ['overview', 'tagline']:
        if col in df.columns:
            df[col] = df[col].replace(['', ' ', 'No Data', 'N/A'], np.nan)

    # 5. Remove duplicates and filter
    if 'id' in df.columns:
        df = df.drop_duplicates(subset=['id'], keep='first')
    
    # Keep rows with enough data (arbitrary threshold from original nb: 10 cols)
    df = df[df.count(axis=1) >= 10]

    # Filter by status
    if 'status' in df.columns:
        df = df[df['status'] == 'Released']
        df = df.drop(columns=['status'], errors='ignore')

    # 6. Parse Credits
    if 'credits' in df.columns:
        df['credits'] = df['credits'].apply(safe_parse)
        df['cast'] = df['credits'].apply(extract_cast)
        df['cast_size'] = df['cast'].apply(len)
        df['director'] = df['credits'].apply(extract_director)
        df['crew_size'] = df['credits'].apply(extract_crew_size)
        # Drop original huge credits column if desired, keeping for now if debugging needed
        # df = df.drop(columns=['credits'])

    # 7. Reorder Columns
    desired_order = [
        'id', 'title', 'tagline', 'overview', 'release_date',
        'genres', 'belongs_to_collection', 'original_language', 'spoken_languages',
        'production_companies', 'production_countries',
        'budget_musd', 'revenue_musd',
        'runtime', 'popularity', 'vote_count', 'vote_average',
        'cast', 'cast_size', 'director', 'crew_size'
    ]
    # Filter only columns that exist
    final_cols = [c for c in desired_order if c in df.columns]
    df = df[final_cols]

    # Reset Index
    df.reset_index(drop=True, inplace=True)
    
    print(f"Data cleaning complete. Final shape: {df.shape}")
    return df
