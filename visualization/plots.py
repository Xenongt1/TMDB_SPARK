
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

def set_style():
    """Sets the visual style for plots."""
    sns.set_style("whitegrid")
    plt.rcParams['figure.figsize'] = (12, 6)
    plt.rcParams['font.size'] = 12

def plot_revenue_vs_budget(df: pd.DataFrame, save_path: str = None, show: bool = True):
    """Scatter plot of Revenue vs Budget."""
    if df.empty or 'budget_musd' not in df.columns or 'revenue_musd' not in df.columns:
        print("Not enough data to plot Revenue vs Budget.")
        return

    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='budget_musd', y='revenue_musd', alpha=0.7)
    
    # Add regression line
    sns.regplot(data=df, x='budget_musd', y='revenue_musd', scatter=False, color='red')
    
    plt.title('Movie Revenue vs Budget', fontsize=16)
    plt.xlabel('Budget (Million USD)')
    plt.ylabel('Revenue (Million USD)')
    
    if save_path:
        plt.savefig(save_path, bbox_inches='tight')
        print(f"Saved plot to {save_path}")
        
    if show:
        plt.show()
    else:
        plt.close()

def plot_roi_by_genre(df: pd.DataFrame, save_path: str = None, show: bool = True):
    """Bar plot of average ROI by genre."""
    if df.empty or 'genres' not in df.columns or 'roi' not in df.columns:
        return

    # Split genres and explode (since genres are | separated strings)
    df_exploded = df.assign(genre=df['genres'].str.split(' | ')).explode('genre')
    
    roi_by_genre = df_exploded.groupby('genre')['roi'].mean().sort_values(ascending=False)
    
    plt.figure(figsize=(14, 8))
    sns.barplot(x=roi_by_genre.values, y=roi_by_genre.index, palette='viridis')
    plt.title('Average ROI by Low-level Genre', fontsize=16)
    plt.xlabel('Average ROI')
    plt.ylabel('Genre')
    
    if save_path:
        plt.savefig(save_path, bbox_inches='tight')
        print(f"Saved plot to {save_path}")

    if show:
        plt.show()
    else:
        plt.close()

def plot_popularity_vs_rating(df: pd.DataFrame, save_path: str = None, show: bool = True):
    """Scatter plot of Popularity vs Rating."""
    if df.empty or 'popularity' not in df.columns or 'vote_average' not in df.columns:
        return

    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='vote_average', y='popularity', hue='vote_count', size='vote_count', sizes=(20, 200), alpha=0.7)
    plt.title('Popularity vs. Rating', fontsize=16)
    plt.xlabel('Vote Average')
    plt.ylabel('Popularity')
    plt.legend(title='Vote Count', bbox_to_anchor=(1.05, 1), loc='upper left')
    
    if save_path:
        plt.savefig(save_path, bbox_inches='tight')
        print(f"Saved plot to {save_path}")

    if show:
        plt.show()
    else:
        plt.close()

def plot_franchise_comparison(franchises_df: pd.DataFrame, save_path: str = None, show: bool = True):
    """Bar plot comparing franchise metrics."""
    if franchises_df.empty:
        return

    # Take top 10 by total revenue
    top_franchises = franchises_df.head(10)
    
    fig, ax1 = plt.subplots(figsize=(12, 6))

    color = 'tab:blue'
    ax1.set_xlabel('Franchise')
    ax1.set_ylabel('Total Revenue (M USD)', color=color)
    sns.barplot(x=top_franchises.index, y='total_revenue', data=top_franchises, ax=ax1, color=color, alpha=0.6)
    ax1.tick_params(axis='y', labelcolor=color)
    plt.xticks(rotation=45, ha='right')

    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:red'
    ax2.set_ylabel('Avg Rating', color=color)  # we already handled the x-label with ax1
    sns.lineplot(x=top_franchises.index, y='avg_rating', data=top_franchises, ax=ax2, color=color, marker='o', sort=False, linewidth=2)
    ax2.tick_params(axis='y', labelcolor=color)

    plt.title('Top Franchises: Revenue vs Rating', fontsize=16)
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    
    if save_path:
        plt.savefig(save_path, bbox_inches='tight')
        print(f"Saved plot to {save_path}")

    if show:
        plt.show()
    else:
        plt.close()

def plot_yearly_trends(df: pd.DataFrame, save_path: str = None, show: bool = True):
    """Line plot of Yearly Trends in Box Office Performance."""
    if df.empty or 'release_date' not in df.columns or 'revenue_musd' not in df.columns:
        return

    df['year'] = df['release_date'].dt.year
    yearly_revenue = df.groupby('year')['revenue_musd'].sum().reset_index()

    plt.figure(figsize=(12, 6))
    sns.lineplot(data=yearly_revenue, x='year', y='revenue_musd', marker='o')
    plt.title('Yearly Trends in Box Office Revenue', fontsize=16)
    plt.xlabel('Year')
    plt.ylabel('Total Revenue (Million USD)')
    
    if save_path:
        plt.savefig(save_path, bbox_inches='tight')
        print(f"Saved plot to {save_path}")

    if show:
        plt.show()
    else:
        plt.close()
