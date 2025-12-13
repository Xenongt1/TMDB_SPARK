
import time
import requests
import pandas as pd
from typing import List, Dict, Optional
from config.settings import BASE_URL, TMDB_API_KEY as API_KEY

def fetch_single_movie(movie_id: int, max_attempts: int = 3) -> Optional[Dict]:
    """
    Fetch details for a single movie by ID with retries.
    """
    url = f"{BASE_URL}/{movie_id}?api_key={API_KEY}&append_to_response=credits"
    attempts = 0
    
    while attempts < max_attempts:
        try:
            response = requests.get(url, timeout=10)
            attempts += 1
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                print(f"Movie ID {movie_id} not found (404). Skipping.")
                return None
            else:
                print(f"Attempt {attempts} for ID {movie_id} failed (status {response.status_code}). Retrying...")
                time.sleep(1)
        except requests.exceptions.RequestException as e:
            attempts += 1
            print(f"Network error on ID {movie_id}: {e}. Retrying...")
            time.sleep(1)
            
    print(f"Failed to fetch movie ID {movie_id} after {max_attempts} attempts.")
    return None

def fetch_movie_data(movie_ids: List[int]) -> pd.DataFrame:
    """
    Fetch data for a list of movie IDs and return a DataFrame.
    """
    movies = []
    print(f"Fetching data for {len(movie_ids)} movies...")
    
    for m_id in movie_ids:
        data = fetch_single_movie(m_id)
        if data:
            movies.append(data)
            
    df = pd.DataFrame(movies)
    print(f"Successfully fetched {len(df)} movies.")
    return df
