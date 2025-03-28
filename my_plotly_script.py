import warnings
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from dash import Dash, html, dcc, Input, Output, State, callback_context
import plotly.express as px
from datetime import datetime
from flask_caching import Cache
import functools

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# ----------------------------
# 1) Create Dash Application with Caching
# ----------------------------
app = Dash(
    __name__, 
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
    ],
    # Reduce the number of callbacks fired during page load
    suppress_callback_exceptions=True
)

# Setup caching to reduce redundant calculations
cache = Cache(app.server, config={
    'CACHE_TYPE': 'simple',
    'CACHE_DEFAULT_TIMEOUT': 300  # 5 minutes cache timeout
})

# ----------------------------
# 2) Optimized Data Loading Functions
# ----------------------------
@cache.memoize()
def load_rolling_avg_data():
    """Load and process rolling average data with caching"""
    try:
        df = pd.read_csv("rolling_time_window_with_category.csv", parse_dates=["timestamp"])
        # Aggregate data by category and timestamp
        cat_monthly = df.groupby(["new_category", "timestamp"])["rolling_avg_rating"].mean().reset_index()
        categories = cat_monthly["new_category"].unique()
        return cat_monthly, categories
    except Exception as e:
        print(f"Error loading rolling average data: {e}")
        return pd.DataFrame(columns=["new_category", "timestamp", "rolling_avg_rating"]), []

@cache.memoize()
def load_top_engagement_data():
    """Load and process top engagement data with caching"""
    try:
        file_path = "TopEngagementResults.csv"
        records = []

        with open(file_path, 'r', encoding='utf-8') as f:
            # Read in chunks to reduce memory usage
            chunk_size = 1000
            lines = []
            for i, line in enumerate(f):
                lines.append(line.strip())
                if i % chunk_size == 0 and i > 0:
                    process_lines(lines, records)
                    lines = []
            
            # Process any remaining lines
            if lines:
                process_lines(lines, records)
        
        df = pd.DataFrame(records)
        categories = df['category'].unique() if not df.empty else []
        return df, categories
    except Exception as e:
        print(f"Error loading top engagement data: {e}")
        return pd.DataFrame(columns=["category", "product_title", "rating"]), []

def process_lines(lines, records):
    """Process a chunk of lines to reduce memory usage"""
    for line in lines:
        if not line:
            continue
        
        # Extract data from CSV-like format
        first_comma = line.find(',')
        last_comma = line.rfind(',')
        if first_comma == -1 or last_comma == -1 or first_comma == last_comma:
            continue
        
        # Extract fields: category, product_title, rating
        category = line[:first_comma]
        product_title = line[first_comma + 1 : last_comma]
        
        try:
            rating = int(line[last_comma + 1 :])
        except ValueError:
            continue
        
        records.append({
            'category': category,
            'product_title': product_title,
            'rating': rating
        })

@cache.memoize()
def load_review_trends_data():
    """Load and process review trends data with caching"""
    try:
        # Load the data from CSV (tab-separated, no header)
        df = pd.read_csv("RatingByYear2.csv", sep="\t", header=None, names=["year_key", "count"])
        
        # Split 'year_key' into 'year' and 'type'
        df[['year', 'type']] = df['year_key'].str.split('_', expand=True)
        df['year'] = df['year'].astype(int)
        
        # Pivot the data to have 'overall', 'five_star', and 'one_star' in columns
        df_pivot = df.pivot(index="year", columns="type", values="count").reset_index()
        
        # Rename columns for better readability
        df_pivot = df_pivot.rename(columns={"overall": "overall", "5": "five_star", "1": "one_star"})
        
        return df_pivot
    except Exception as e:
        print(f"Error loading review trends data: {e}")
        return pd.DataFrame(columns=["year", "overall", "five_star", "one_star"])

@cache.memoize()
def load_product_settings_data():
    """Load all product settings data with caching"""
    # Initialize empty DataFrames
    df_images_filtered = pd.DataFrame(columns=["image_count", "rating_number"])
    df_features_merged = pd.DataFrame(columns=["feature_count", "rating_number"])
    df_desc = pd.DataFrame(columns=["description_count", "rating_number"])
    
    # Load image count data
    try:
        df_images = pd.read_csv("ProductMetadataAnalysisResults.csv", sep="\t", header=None, 
                         names=["parent_asin", "metrics"])
        
        # Split the 'metrics' column
        df_images[['image_count', 'rating_number', 'video_count']] = df_images['metrics'].str.split(",", expand=True)
        
        # Convert columns to numeric types
        df_images['image_count'] = pd.to_numeric(df_images['image_count'], errors='coerce')
        df_images['rating_number'] = pd.to_numeric(df_images['rating_number'], errors='coerce')
        
        # Drop rows with missing values
        df_images.dropna(subset=['image_count', 'rating_number'], inplace=True)
        
        # Filter to include only products with image_count from 0 to 20
        # Also sample data to reduce size if it's very large
        if len(df_images) > 5000:
            df_images = df_images.sample(5000, random_state=42)
            
        df_images_filtered = df_images[(df_images['image_count'] >= 0) & (df_images['image_count'] <= 20)]
        
        # Load feature count data
        try:
            df_features = pd.read_csv("ProductFeaturesAnalysisResults.csv",
                              sep="\t", header=None, names=["parent_asin", "feature_count"])
            
            # Convert feature_count to numeric
            df_features["feature_count"] = pd.to_numeric(df_features["feature_count"], errors="coerce")
            
            # Merge with ratings data
            df_features_merged = pd.merge(df_features[["parent_asin", "feature_count"]],
                             df_images[["parent_asin", "rating_number"]],
                             on="parent_asin", how="inner")
            
            # Drop rows where we don't have valid data
            df_features_merged.dropna(subset=["feature_count", "rating_number"], inplace=True)
            
            # Sample data to reduce size if it's very large
            if len(df_features_merged) > 5000:
                df_features_merged = df_features_merged.sample(5000, random_state=42)
        except Exception as e:
            print(f"Error loading feature count data: {e}")
        
        # Load description count data
        try:
            df_desc = pd.read_csv("ProductDescriptionAnalysisResults.csv", 
                         sep="\t", header=None, 
                         names=["parent_asin", "desc_rating"])
            
            # Split the "desc_rating" column into two separate columns
            df_desc[['description_count', 'rating_number']] = df_desc['desc_rating'].str.split(",", expand=True)
            
            # Convert the new columns to numeric types
            df_desc["description_count"] = pd.to_numeric(df_desc["description_count"], errors="coerce")
            df_desc["rating_number"] = pd.to_numeric(df_desc["rating_number"], errors="coerce")
            
            # Drop rows with missing values
            df_desc.dropna(subset=["description_count", "rating_number"], inplace=True)
            
            # Sample data to reduce size if it's very large
            if len(df_desc) > 5000:
                df_desc = df_desc.sample(5000, random_state=42)
        except Exception as e:
            print(f"Error loading description count data: {e}")
    except Exception as e:
        print(f"Error loading image count data: {e}")
    
    return df_images_filtered, df_features_merged, df_desc

# Load data at startup to avoid loading during callbacks
cat_monthly, categories1 = load_rolling_avg_data()
df2, categories2 = load_top_engagement_data()
df_pivot = load_review_trends_data()
df_images_filtered, df_features_merged, df_desc = load_product_settings_data()

# Forecast periods - reduced from 240 to 120 to decrease computation
future_periods = 120

# ----------------------------
# 1) Load Data for Visualization 1 (Rolling Average Rating)
# ----------------------------
#df1 = pd.read_csv("rolling_time_window_with_category.csv", parse_dates=["timestamp"])

# Aggregate data by category and timestamp
#cat_monthly = df1.groupby(["new_category", "timestamp"])["rolling_avg_rating"].mean().reset_index()

# Forecast 24 months (2 years) beyond the last available timestamp
#future_periods = 240
#categories1 = cat_monthly["new_category"].unique()

# ----------------------------
# 2) Load Data for Visualization 2 (Top 5 Most User-Engaged Products)
# ----------------------------
#file_path2 = "TopEngagementResults.csv"  # Adjust if needed
#records = []

#with open(file_path2, 'r', encoding='utf-8') as f:
#    for line in f:
#        line = line.strip()
#        if not line:
#            continue
        
        # Extract data from CSV-like format
#        first_comma = line.find(',')
#        last_comma = line.rfind(',')
#        if first_comma == -1 or last_comma == -1 or first_comma == last_comma:
#            continue
        
        # Extract fields: category, product_title, rating
#        category = line[:first_comma]
#        product_title = line[first_comma + 1 : last_comma]
        
#        try:
#            rating = int(line[last_comma + 1 :])
#        except ValueError:
#            continue
        
#        records.append({
#            'category': category,
#            'product_title': product_title,
#            'rating': rating
#        })

#df2 = pd.DataFrame(records)
#categories2 = df2['category'].unique()

# ----------------------------
# 3) Load Data for Visualization 3 (Review Trends Over the Years)
# ----------------------------
# Load the data from CSV (tab-separated, no header)
#df3 = pd.read_csv("RatingByYear2.csv", sep="\t", header=None, names=["year_key", "count"])

# Split 'year_key' into 'year' and 'type' (e.g., "1998_overall" becomes "1998" and "overall")
#df3[['year', 'type']] = df3['year_key'].str.split('_', expand=True)
#df3['year'] = df3['year'].astype(int)

# Pivot the data to have 'overall', 'five_star', and 'one_star' in columns
#df_pivot = df3.pivot(index="year", columns="type", values="count").reset_index()

# Rename columns for better readability
#df_pivot = df_pivot.rename(columns={"overall": "overall", "5": "five_star", "1": "one_star"})

# ----------------------------
# 4) Load Data for Product Setting Recommendations
# ----------------------------
# Load image count data
#try:
#    df_images = pd.read_csv("ProductMetadataAnalysisResults.csv", sep="\t", header=None, 
#                     names=["parent_asin", "metrics"])
    
    # Split the 'metrics' column
#    df_images[['image_count', 'rating_number', 'video_count']] = df_images['metrics'].str.split(",", expand=True)
    
    # Convert columns to numeric types
#    df_images['image_count'] = pd.to_numeric(df_images['image_count'], errors='coerce')
#    df_images['rating_number'] = pd.to_numeric(df_images['rating_number'], errors='coerce')
#    df_images['video_count'] = pd.to_numeric(df_images['video_count'], errors='coerce')
    
    # Drop rows with missing values
#    df_images.dropna(inplace=True)
    
    # Filter to include only products with image_count from 0 to 20
#    df_images_filtered = df_images[(df_images['image_count'] >= 0) & (df_images['image_count'] <= 20)]
#except Exception as e:
#    print(f"Error loading image count data: {e}")
#    df_images_filtered = pd.DataFrame(columns=["image_count", "rating_number"])

# Load feature count data
#try:
#    df_features = pd.read_csv("ProductFeaturesAnalysisResults.csv",
#                          sep="\t", header=None, names=["parent_asin", "feature_count"])
    
    # Convert feature_count to numeric
#    df_features["feature_count"] = pd.to_numeric(df_features["feature_count"], errors="coerce")
    
    # Load ratings data if not already loaded with image count
#    if 'df_images' not in locals():
#        df_ratings = pd.read_csv("ProductMetadataAnalysisResults.csv",
#                             sep="\t", header=None, names=["parent_asin", "metrics"])
        
        # Split out the metrics into separate columns
#        df_ratings[["image_count", "rating_number", "video_count"]] = df_ratings["metrics"].str.split(",", expand=True)
        
        # Convert rating_number to numeric
#        df_ratings["rating_number"] = pd.to_numeric(df_ratings["rating_number"], errors="coerce")
#    else:
#        df_ratings = df_images.copy()
    
    # Merge the two DataFrames on parent_asin
#    df_features_merged = pd.merge(df_features[["parent_asin", "feature_count"]],
#                         df_ratings[["parent_asin", "rating_number"]],
#                         on="parent_asin", how="inner")
    
    # Drop rows where we don't have valid data
#    df_features_merged.dropna(subset=["feature_count", "rating_number"], inplace=True)
#except Exception as e:
#    print(f"Error loading feature count data: {e}")
#    df_features_merged = pd.DataFrame(columns=["feature_count", "rating_number"])

# Load description count data
#try:
#    df_desc = pd.read_csv("ProductDescriptionAnalysisResults.csv", 
#                     sep="\t", header=None, 
#                     names=["parent_asin", "desc_rating"])
    
    # Split the "desc_rating" column into two separate columns
#    df_desc[['description_count', 'rating_number']] = df_desc['desc_rating'].str.split(",", expand=True)
    
    # Convert the new columns to numeric types
#    df_desc["description_count"] = pd.to_numeric(df_desc["description_count"], errors="coerce")
#    df_desc["rating_number"] = pd.to_numeric(df_desc["rating_number"], errors="coerce")
    
    # Drop rows with missing values
#    df_desc.dropna(subset=["description_count", "rating_number"], inplace=True)
#except Exception as e:
#    print(f"Error loading description count data: {e}")
#    df_desc = pd.DataFrame(columns=["description_count", "rating_number"])

# --------------------------------------------
# 5) Create Dash Application with Modern UI
# --------------------------------------------
#app = Dash(
#    __name__, 
#    meta_tags=[
#        {"name": "viewport", "content": "width=device-width, initial-scale=1.0"}
#    ]
#)

# Custom CSS for better styling
app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        {%metas%}
        <title>Product Analytics Dashboard</title>
        {%favicon%}
        {%css%}
        <style>
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
            
            body {
                font-family: 'Inter', sans-serif;
                margin: 0;
                background-color: #f8f9fa;
                color: #212529;
            }
            
            .dashboard-container {
                max-width: 1400px;
                margin: 0 auto;
                padding: 20px;
            }
            
            .header {
                background-color: #4361ee;
                color: white;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 20px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            
            .header h1 {
                margin: 0;
                font-weight: 600;
                font-size: 24px;
            }
            
            .header p {
                margin: 5px 0 0 0;
                opacity: 0.9;
                font-weight: 300;
            }
            
            .card {
                background: white;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
                padding: 20px;
                margin-bottom: 20px;
                transition: transform 0.2s, box-shadow 0.2s;
            }
            
            .card:hover {
                transform: translateY(-2px);
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);
            }
            
            .card-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                margin-bottom: 15px;
            }
            
            .card-title {
                font-size: 16px;
                font-weight: 600;
                color: #212529;
                margin: 0;
            }
            
            .dropdown-container {
                width: 300px;
            }
            
            .dash-dropdown {
                border: 1px solid #e9ecef;
                border-radius: 4px;
            }
            
            .graph-container {
                min-height: 400px;
            }
            
            .footer {
                text-align: center;
                padding: 20px;
                color: #6c757d;
                font-size: 14px;
            }
            
            @media (max-width: 768px) {
                .flex-container {
                    flex-direction: column;
                }
                
                .card {
                    width: 100% !important;
                }
                
                .dropdown-container {
                    width: 100%;
                }
            }
            
            .section-header {
                margin: 30px 0 15px 0;
                font-size: 20px;
                font-weight: 600;
                color: #4361ee;
                border-bottom: 2px solid #e9ecef;
                padding-bottom: 10px;
            }
        </style>
    </head>
    <body>
        {%app_entry%}
        <footer>
            {%config%}
            {%scripts%}
            {%renderer%}
        </footer>
    </body>
</html>
'''

# App layout with improved UI
app.layout = html.Div(className="dashboard-container", children=[
    # Header
    html.Div(className="header", children=[
        html.H1("Product Analytics Dashboard"),
        html.P(f"Last updated: {datetime.now().strftime('%B %d, %Y')}"),
    ]),
    
    # Main content - First row
    html.Div(className="flex-container", style={
        "display": "flex", 
        "flexWrap": "wrap", 
        "gap": "20px"
    }, children=[
        # Visualization 1: Rolling Average Rating
        html.Div(className="card", style={"width": "calc(50% - 10px)"}, children=[
            html.Div(className="card-header", children=[
                html.H2(className="card-title", children="Product Rating Trends & Forecast"),
                html.Div(className="dropdown-container", children=[
                    html.Label("Select Category:", style={"marginBottom": "5px", "display": "block", "fontSize": "14px"}),
                    dcc.Dropdown(
                        id="category-dropdown-1",
                        options=[{"label": cat, "value": cat} for cat in categories1],
                        value=categories1[0] if len(categories1) > 0 else None,
                        clearable=False,
                        className="dash-dropdown"
                    ),
                ]),
            ]),
            html.Div(className="graph-container", children=[
                dcc.Loading(
                    type="circle",
                    children=dcc.Graph(
                        id="graph-1",
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'responsive': True
                        }
                    )
                )
            ]),
            html.Div(style={"fontSize": "13px", "color": "#6c757d", "marginTop": "10px"}, children=[
                html.P("This chart shows historical rating data with a forecast for future trends.")
            ])
        ]),

        # Visualization 2: Top 5 Products
        html.Div(className="card", style={"width": "calc(50% - 10px)"}, children=[
            html.Div(className="card-header", children=[
                html.H2(className="card-title", children="Top 5 User-Engaged Products"),
                html.Div(className="dropdown-container", children=[
                    html.Label("Select Category:", style={"marginBottom": "5px", "display": "block", "fontSize": "14px"}),
                    dcc.Dropdown(
                        id="category-dropdown-2",
                        options=[{"label": cat, "value": cat} for cat in categories2],
                        value=categories2[0] if len(categories2) > 0 else None,
                        clearable=False,
                        className="dash-dropdown"
                    ),
                ]),
            ]),
            html.Div(className="graph-container", children=[
                dcc.Loading(
                    type="circle",
                    children=dcc.Graph(
                        id="graph-2",
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'responsive': True
                        }
                    )
                )
            ]),
            html.Div(style={"fontSize": "13px", "color": "#6c757d", "marginTop": "10px"}, children=[
                html.P("This chart shows the top 5 products with the highest user engagement.")
            ])
        ]),
    ]),
    
    # Second row - Review Trends Visualization
    html.Div(className="flex-container", style={
        "display": "flex", 
        "flexWrap": "wrap", 
        "gap": "20px"
    }, children=[
        # Visualization 3: Review Trends Over the Years
        html.Div(className="card", style={"width": "100%"}, children=[
            html.Div(className="card-header", children=[
                html.H2(className="card-title", children="Review Trends Over the Years"),
                html.Div(className="dropdown-container", children=[
                    html.Label("Select Review Type:", style={"marginBottom": "5px", "display": "block", "fontSize": "14px"}),
                    dcc.Dropdown(
                        id="review-dropdown",
                        options=[
                            {"label": "All Review Types Combined", "value": "combined"},
                            {"label": "Overall Reviews", "value": "overall"},
                            {"label": "Five Star Reviews", "value": "five_star"},
                            {"label": "One Star Reviews", "value": "one_star"}
                        ],
                        value="combined",  # Default to combined view
                        clearable=False,
                        className="dash-dropdown"
                    ),
                ]),
            ]),
            html.Div(className="graph-container", children=[
                dcc.Loading(
                    type="circle",
                    children=dcc.Graph(
                        id="review-graph",
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'responsive': True
                        }
                    )
                )
            ]),
            html.Div(style={"fontSize": "13px", "color": "#6c757d", "marginTop": "10px"}, children=[
                html.P("This chart shows how review patterns have changed over time, helping identify trends in customer satisfaction.")
            ])
        ]),
    ]),
    
    # New Section - Product Setting Recommendations
    html.H2(className="section-header", children="Product Setting Recommendations"),
    html.Div(className="flex-container", style={
        "display": "flex", 
        "flexWrap": "wrap", 
        "gap": "20px"
    }, children=[
        # Visualization 4: Feature Count vs. Rating Number
        html.Div(className="card", style={"width": "calc(33.33% - 14px)"}, children=[
            html.Div(className="card-header", children=[
                html.H2(className="card-title", children="Feature Count Impact"),
            ]),
            html.Div(className="graph-container", children=[
                dcc.Loading(
                    type="circle",
                    children=dcc.Graph(
                        id="feature-count-graph",
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'responsive': True
                        }
                    )
                )
            ]),
            html.Div(style={"fontSize": "13px", "color": "#6c757d", "marginTop": "10px"}, children=[
                html.P("This chart shows how the number of product features correlates with review count.")
            ])
        ]),
        
        # Visualization 5: Description Count vs. Rating Number
        html.Div(className="card", style={"width": "calc(33.33% - 14px)"}, children=[
            html.Div(className="card-header", children=[
                html.H2(className="card-title", children="Description Length Impact"),
            ]),
            html.Div(className="graph-container", children=[
                dcc.Loading(
                    type="circle",
                    children=dcc.Graph(
                        id="description-count-graph",
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'responsive': True
                        }
                    )
                )
            ]),
            html.Div(style={"fontSize": "13px", "color": "#6c757d", "marginTop": "10px"}, children=[
                html.P("This chart shows how description length correlates with review count.")
            ])
        ]),
        
        # Visualization 6: Image Count vs. Rating Number
        html.Div(className="card", style={"width": "calc(33.33% - 14px)"}, children=[
            html.Div(className="card-header", children=[
                html.H2(className="card-title", children="Image Count Impact"),
            ]),
            html.Div(className="graph-container", children=[
                dcc.Loading(
                    type="circle",
                    children=dcc.Graph(
                        id="image-count-graph",
                        config={
                            'displayModeBar': True,
                            'displaylogo': False,
                            'modeBarButtonsToRemove': ['lasso2d', 'select2d'],
                            'responsive': True
                        }
                    )
                )
            ]),
            html.Div(style={"fontSize": "13px", "color": "#6c757d", "marginTop": "10px"}, children=[
                html.P("This chart shows how the number of product images correlates with review count.")
            ])
        ]),
    ]),
    
    # Footer
    html.Div(className="footer", children=[
        html.P("© 2025 Product Analytics Dashboard. All data is updated daily.")
    ]),
    
    # Store component to prevent redundant data loading
    dcc.Store(id='product-settings-data-loaded', data=False),
])


# --------------------------------------------
# 6) Optimized Callbacks
# --------------------------------------------
# Helper function for empty charts
def empty_chart(message="No data available"):
    fig = go.Figure()
    fig.update_layout(
        title=None,
        xaxis={"visible": False},
        yaxis={"visible": False},
        annotations=[
            {
                "text": message,
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {
                    "size": 16
                }
            }
        ],
        plot_bgcolor="white",
        height=400
    )
    return fig

@app.callback(
    Output("graph-1", "figure"),
    [Input("category-dropdown-1", "value")]
)
@cache.memoize()
def update_graph_1(selected_category):
    if selected_category is None:
        return empty_chart("Please select a category")
    
    # Filter data for selected category
    data = cat_monthly[cat_monthly["new_category"] == selected_category].sort_values("timestamp").copy()
    
    if len(data) < 2 or data["rolling_avg_rating"].isna().all():
        return empty_chart("No data available for this category")

    # Convert timestamp to numeric
    data["time_numeric"] = data["timestamp"].map(pd.Timestamp.toordinal)
    
    X = data["time_numeric"].values
    y = data["rolling_avg_rating"].values
    
    # Check if we have at least two distinct timestamps and ratings
    if len(np.unique(X)) < 2 or len(np.unique(y[~np.isnan(y)])) < 2:
        return empty_chart("Insufficient data for trend analysis")

    # Fit a linear model
    try:
        coef = np.polyfit(X, y, 1)  # slope, intercept
        poly1d_fn = np.poly1d(coef)
    except Exception:
        return empty_chart("Error in trend calculation")

    # Forecast future months - reduced number of points for better performance
    last_date = data["timestamp"].max()
    future_dates = [last_date + pd.DateOffset(months=i) for i in range(1, future_periods + 1, 2)]  # Step by 2 months
    future_numeric = np.array([d.toordinal() for d in future_dates])
    future_preds = poly1d_fn(future_numeric)

    # Create traces with improved styling
    hist_trace = go.Scatter(
        x=data["timestamp"],
        y=data["rolling_avg_rating"],
        mode="lines+markers",
        name=f"Historical Data",
        line=dict(color="#4361ee", width=2),
        marker=dict(size=6, color="#4361ee", line=dict(width=1, color="#ffffff")),
    )

    forecast_trace = go.Scatter(
        x=future_dates,
        y=future_preds,
        mode="lines",
        name=f"Forecast",
        line=dict(color="#ff6b6b", width=2, dash="dash"),
    )

    # Create figure layout with improved styling
    fig = go.Figure(data=[hist_trace, forecast_trace])
    fig.update_layout(
        title=None,
        xaxis_title="Time Period",
        yaxis_title="Average Rating",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        font=dict(family="Inter, sans-serif"),
        plot_bgcolor="white",
        margin=dict(l=10, r=10, t=10, b=10),
        hovermode="x unified",
    )
    
    # Add grid lines and improve axis styling
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
        tickformat="%Y",
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
        range=[max(0, min(data["rolling_avg_rating"].min() - 0.5, 3)), 
               min(5, data["rolling_avg_rating"].max() + 0.5)]
    )
    
    # Add a subtle annotation for the category
    fig.add_annotation(
        text=f"Category: {selected_category}",
        xref="paper", yref="paper",
        x=0.01, y=0.99,
        showarrow=False,
        font=dict(size=12, color="#6c757d"),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor="rgba(0,0,0,0.1)",
        borderwidth=1,
        borderpad=4,
        opacity=0.8
    )

    return fig


@app.callback(
    Output("graph-2", "figure"),
    [Input("category-dropdown-2", "value")]
)
@cache.memoize()
def update_graph_2(selected_category):
    if selected_category is None:
        return empty_chart("Please select a category")

    # Filter data for selected category
    group_data = df2[df2["category"] == selected_category].copy()
    top5 = group_data.sort_values(by="rating", ascending=False).head(5)

    if top5.empty:
        return empty_chart("No data available for this category")

    # Create color gradient for bars
    colors = px.colors.sequential.Blues[3:8]
    
    # Create bar chart for top 5 products with improved styling
    trace = go.Bar(
        y=top5["product_title"],
        x=top5["rating"],
        orientation="h",
        name=selected_category,
        marker=dict(
            color=colors,
            line=dict(width=0)
        ),
        hovertemplate="<b>%{y}</b><br>Ratings: %{x}<extra></extra>"
    )

    # Create figure layout with improved styling
    fig = go.Figure(data=[trace])
    fig.update_layout(
        title=None,
        xaxis_title="Number of Ratings",
        yaxis_title=None,
        margin=dict(l=10, r=10, t=10, b=10),
        height=400,
        font=dict(family="Inter, sans-serif"),
        plot_bgcolor="white",
        showlegend=False,
    )
    
    # Improve axis styling
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
    )
    
    fig.update_yaxes(
        showgrid=False,
        zeroline=False,
        automargin=True,
    )
    
    # Add a subtle annotation for the category
    fig.add_annotation(
        text=f"Category: {selected_category}",
        xref="paper", yref="paper",
        x=0.01, y=0.99,
        showarrow=False,
        font=dict(size=12, color="#6c757d"),
        bgcolor="rgba(255,255,255,0.8)",
        bordercolor="rgba(0,0,0,0.1)",
        borderwidth=1,
        borderpad=4,
        opacity=0.8
    )
    
    # Add value labels to the bars
    for i, value in enumerate(top5["rating"]):
        fig.add_annotation(
            x=value,
            y=top5["product_title"].iloc[i],
            text=f"{value}",
            showarrow=False,
            xshift=5,
            font=dict(color="white" if value > max(top5["rating"]) * 0.3 else "#333"),
            xanchor="left"
        )

    return fig


@app.callback(
    Output("review-graph", "figure"),
    [Input("review-dropdown", "value")]
)
@cache.memoize()
def update_review_graph(selected_type):
    if selected_type is None:
        return empty_chart("Please select a review type")
    
    # Set colors for different review types
    color_map = {
        "overall": "#4361ee",  # Blue
        "five_star": "#2e8b57",  # Green
        "one_star": "#ff6b6b"   # Red
    }
    
    # Create figure
    fig = go.Figure()
    
    # If combined view is selected, show all three lines
    if selected_type == "combined":
        # Add trace for overall reviews
        fig.add_trace(
            go.Scatter(
                x=df_pivot["year"],
                y=df_pivot["overall"],
                mode="lines+markers",
                name="All Reviews",
                line=dict(color=color_map["overall"], width=3),
                marker=dict(size=8, color=color_map["overall"], line=dict(width=1, color="#ffffff")),
                hovertemplate="<b>Year: %{x}</b><br>Reviews: %{y:,}<extra>All Reviews</extra>"
            )
        )
        
        # Add trace for five-star reviews
        fig.add_trace(
            go.Scatter(
                x=df_pivot["year"],
                y=df_pivot["five_star"],
                mode="lines+markers",
                name="5★ Reviews",
                line=dict(color=color_map["five_star"], width=3),
                marker=dict(size=8, color=color_map["five_star"], line=dict(width=1, color="#ffffff")),
                hovertemplate="<b>Year: %{x}</b><br>Reviews: %{y:,}<extra>5★ Reviews</extra>"
            )
        )
        
        # Add trace for one-star reviews
        fig.add_trace(
            go.Scatter(
                x=df_pivot["year"],
                y=df_pivot["one_star"],
                mode="lines+markers",
                name="1★ Reviews",
                line=dict(color=color_map["one_star"], width=3),
                marker=dict(size=8, color=color_map["one_star"], line=dict(width=1, color="#ffffff")),
                hovertemplate="<b>Year: %{x}</b><br>Reviews: %{y:,}<extra>1★ Reviews</extra>"
            )
        )
        
        # Update layout for combined view
        fig.update_layout(
            title=None,
            xaxis_title="Year",
            yaxis_title="Review Count",
            font=dict(family="Inter, sans-serif"),
            plot_bgcolor="white",
            margin=dict(l=10, r=10, t=10, b=10),
            height=450,
            hovermode="closest",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1
            )
        )
        
        # Add annotation for combined view
        fig.add_annotation(
            text="Review Types Comparison",
            xref="paper", yref="paper",
            x=0.01, y=0.99,
            showarrow=False,
            font=dict(size=14, color="#333", weight="bold"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="rgba(0,0,0,0.1)",
            borderwidth=1,
            borderpad=4,
            opacity=0.9
        )
        
        # Calculate and add insights about the relationship between review types
        if len(df_pivot) >= 2:
            # Get the most recent year data
            latest_year = df_pivot["year"].max()
            latest_data = df_pivot[df_pivot["year"] == latest_year]
            
            if not latest_data.empty:
                five_star_percent = (latest_data["five_star"].values[0] / latest_data["overall"].values[0] * 100) if latest_data["overall"].values[0] > 0 else 0
                one_star_percent = (latest_data["one_star"].values[0] / latest_data["overall"].values[0] * 100) if latest_data["overall"].values[0] > 0 else 0
                
                # Add annotation with insights
                fig.add_annotation(
                    text=f"Latest Year ({latest_year}): 5★ {five_star_percent:.1f}% | 1★ {one_star_percent:.1f}% of total",
                    xref="paper", yref="paper",
                    x=0.99, y=0.01,
                    showarrow=False,
                    font=dict(size=12, color="#333"),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="rgba(0,0,0,0.1)",
                    borderwidth=1,
                    borderpad=4,
                    opacity=0.9,
                    align="right",
                    xanchor="right"
                )
    else:
        # Original code for individual review types
        title_map = {
            "overall": "Overall Reviews Over the Years",
            "five_star": "Five Star Reviews Over the Years",
            "one_star": "One Star Reviews Over the Years"
        }
        
        # Add area under the line for better visual impact
        fig.add_trace(
            go.Scatter(
                x=df_pivot["year"],
                y=df_pivot[selected_type],
                mode="lines+markers",
                name=title_map[selected_type],
                line=dict(
                    color=color_map[selected_type], 
                    width=3
                ),
                marker=dict(
                    size=8, 
                    color=color_map[selected_type],
                    line=dict(width=1, color="#ffffff")
                ),
                fill='tozeroy',
                fillcolor=f"rgba({','.join(str(int(c)) for c in px.colors.hex_to_rgb(color_map[selected_type]))},0.1)",
                hovertemplate="<b>Year: %{x}</b><br>Reviews: %{y:,}<extra></extra>"
            )
        )
        
        # Customize layout with improved styling
        fig.update_layout(
            title=None,
            xaxis_title="Year",
            yaxis_title="Review Count",
            font=dict(family="Inter, sans-serif"),
            plot_bgcolor="white",
            margin=dict(l=10, r=10, t=10, b=10),
            height=450,
            hovermode="x unified",
            showlegend=False
        )
        
        # Add annotations for key insights
        max_year = df_pivot.loc[df_pivot[selected_type].idxmax(), "year"]
        max_value = df_pivot[selected_type].max()
        
        # Add annotation for maximum point
        fig.add_annotation(
            x=max_year,
            y=max_value,
            text=f"Peak: {int(max_value):,}",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor=color_map[selected_type],
            ax=0,
            ay=-40,
            font=dict(size=12, color="#333"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor=color_map[selected_type],
            borderwidth=1,
            borderpad=4
        )
        
        # Add annotation for review type
        review_type_labels = {
            "overall": "All Reviews",
            "five_star": "5★ Reviews",
            "one_star": "1★ Reviews"
        }
        
        fig.add_annotation(
            text=review_type_labels[selected_type],
            xref="paper", yref="paper",
            x=0.01, y=0.99,
            showarrow=False,
            font=dict(size=14, color="#333", weight="bold"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor=color_map[selected_type],
            borderwidth=2,
            borderpad=4,
            opacity=0.9
        )
    
    # Common axis styling for both views
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
        tickmode="linear",
        tick0=df_pivot["year"].min(),
        dtick=1  # Ensure yearly intervals
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
        rangemode="tozero"
    )
    
    return fig


# --------------------------------------------
# 9) Optimized Callbacks for Product Setting Recommendations
# --------------------------------------------
# Use a single callback for all product setting graphs to reduce overhead
@app.callback(
    [Output("feature-count-graph", "figure"),
     Output("description-count-graph", "figure"),
     Output("image-count-graph", "figure")],
    [Input("product-settings-data-loaded", "data")]
)
@cache.memoize()
def update_product_settings_graphs(_):
    # Create all three figures at once to reduce overhead
    feature_fig = create_feature_count_graph()
    desc_fig = create_description_count_graph()
    image_fig = create_image_count_graph()
    
    return feature_fig, desc_fig, image_fig


def create_feature_count_graph():
    # Create scatter plot for feature count vs. rating number
    fig = go.Figure()
    
    if not df_features_merged.empty:
        # Reduce number of points for better performance
        plot_data = df_features_merged
        if len(plot_data) > 1000:
            plot_data = plot_data.sample(1000, random_state=42)
            
        fig.add_trace(
            go.Scatter(
                x=plot_data["feature_count"],
                y=plot_data["rating_number"],
                mode="markers",
                marker=dict(
                    color="#9c27b0",  # Purple
                    size=6,  # Reduced marker size
                    opacity=0.7,
                    line=dict(width=1, color="#ffffff")
                ),
                hovertemplate="<b>Feature Count:</b> %{x}<br><b>Reviews:</b> %{y:,}<extra></extra>"
            )
        )
        
        # Add trendline
        if len(plot_data) > 1:
            try:
                # Calculate trendline
                z = np.polyfit(plot_data["feature_count"], plot_data["rating_number"], 1)
                p = np.poly1d(z)
                
                # Get x range for trendline (use fewer points)
                x_min = plot_data["feature_count"].min()
                x_max = plot_data["feature_count"].max()
                x_range = np.linspace(x_min, x_max, 20)  # Reduced number of points
                
                # Add trendline to plot
                fig.add_trace(
                    go.Scatter(
                        x=x_range,
                        y=p(x_range),
                        mode="lines",
                        name="Trend",
                        line=dict(color="#6a1b9a", width=2, dash="dash"),
                        hoverinfo="skip"
                    )
                )
                
                # Calculate correlation
                correlation = np.corrcoef(plot_data["feature_count"], plot_data["rating_number"])[0, 1]
                
                # Add correlation annotation
                fig.add_annotation(
                    text=f"Correlation: {correlation:.2f}",
                    xref="paper", yref="paper",
                    x=0.98, y=0.02,
                    showarrow=False,
                    font=dict(size=12, color="#333"),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="rgba(0,0,0,0.1)",
                    borderwidth=1,
                    borderpad=4,
                    align="right",
                    xanchor="right"
                )
            except Exception as e:
                print(f"Error calculating trendline: {e}")
    
    # Update layout
    fig.update_layout(
        title=None,
        xaxis_title="Number of Product Features",
        yaxis_title="Number of Reviews",
        font=dict(family="Inter, sans-serif"),
        plot_bgcolor="white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=400,
        showlegend=False
    )
    
    # Improve axis styling
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False
    )
    
    # Add insight annotation
    if not df_features_merged.empty:
        # Find optimal feature count (highest average rating)
        feature_groups = df_features_merged.groupby("feature_count")["rating_number"].mean().reset_index()
        if not feature_groups.empty:
            optimal_features = feature_groups.loc[feature_groups["rating_number"].idxmax(), "feature_count"]
            
            fig.add_annotation(
                text=f"Optimal Feature Count: ~{int(optimal_features)}",
                xref="paper", yref="paper",
                x=0.01, y=0.98,
                showarrow=False,
                font=dict(size=12, color="#333", weight="bold"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="#9c27b0",
                borderwidth=1,
                borderpad=4
            )
    
    return fig


def create_description_count_graph():
    # Create scatter plot for description count vs. rating number
    fig = go.Figure()
    
    if not df_desc.empty:
        # Reduce number of points for better performance
        plot_data = df_desc
        if len(plot_data) > 1000:
            plot_data = plot_data.sample(1000, random_state=42)
            
        fig.add_trace(
            go.Scatter(
                x=plot_data["description_count"],
                y=plot_data["rating_number"],
                mode="markers",
                marker=dict(
                    color="#009688",  # Teal
                    size=6,  # Reduced marker size
                    opacity=0.7,
                    line=dict(width=1, color="#ffffff")
                ),
                hovertemplate="<b>Description Length:</b> %{x}<br><b>Reviews:</b> %{y:,}<extra></extra>"
            )
        )
        
        # Add trendline
        if len(plot_data) > 1:
            try:
                # Calculate trendline
                z = np.polyfit(plot_data["description_count"], plot_data["rating_number"], 1)
                p = np.poly1d(z)
                
                # Get x range for trendline (use fewer points)
                x_min = plot_data["description_count"].min()
                x_max = plot_data["description_count"].max()
                x_range = np.linspace(x_min, x_max, 20)  # Reduced number of points
                
                # Add trendline to plot
                fig.add_trace(
                    go.Scatter(
                        x=x_range,
                        y=p(x_range),
                        mode="lines",
                        name="Trend",
                        line=dict(color="#00695c", width=2, dash="dash"),
                        hoverinfo="skip"
                    )
                )
                
                # Calculate correlation
                correlation = np.corrcoef(plot_data["description_count"], plot_data["rating_number"])[0, 1]
                
                # Add correlation annotation
                fig.add_annotation(
                    text=f"Correlation: {correlation:.2f}",
                    xref="paper", yref="paper",
                    x=0.98, y=0.02,
                    showarrow=False,
                    font=dict(size=12, color="#333"),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="rgba(0,0,0,0.1)",
                    borderwidth=1,
                    borderpad=4,
                    align="right",
                    xanchor="right"
                )
            except Exception as e:
                print(f"Error calculating trendline: {e}")
    
    # Update layout
    fig.update_layout(
        title=None,
        xaxis_title="Description Length (Characters)",
        yaxis_title="Number of Reviews",
        font=dict(family="Inter, sans-serif"),
        plot_bgcolor="white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=400,
        showlegend=False
    )
    
    # Improve axis styling
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False
    )
    
    # Add insight annotation
    if not df_desc.empty:
        # Find optimal description length (highest average rating)
        # Group by ranges of 100 characters for better analysis
        df_desc['desc_range'] = (df_desc['description_count'] // 100) * 100
        desc_groups = df_desc.groupby("desc_range")["rating_number"].mean().reset_index()
        
        if not desc_groups.empty:
            optimal_desc = desc_groups.loc[desc_groups["rating_number"].idxmax(), "desc_range"]
            
            fig.add_annotation(
                text=f"Optimal Description Length: ~{int(optimal_desc)}-{int(optimal_desc)+100} chars",
                xref="paper", yref="paper",
                x=0.01, y=0.98,
                showarrow=False,
                font=dict(size=12, color="#333", weight="bold"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="#009688",
                borderwidth=1,
                borderpad=4
            )
    
    return fig


def create_image_count_graph():
    # Create scatter plot for image count vs. rating number
    fig = go.Figure()
    
    if not df_images_filtered.empty:
        # Reduce number of points for better performance
        plot_data = df_images_filtered
        if len(plot_data) > 1000:
            plot_data = plot_data.sample(1000, random_state=42)
            
        fig.add_trace(
            go.Scatter(
                x=plot_data["image_count"],
                y=plot_data["rating_number"],
                mode="markers",
                marker=dict(
                    color="#1976d2",  # Blue
                    size=6,  # Reduced marker size
                    opacity=0.7,
                    line=dict(width=1, color="#ffffff")
                ),
                hovertemplate="<b>Image Count:</b> %{x}<br><b>Reviews:</b> %{y:,}<extra></extra>"
            )
        )
        
        # Add trendline
        if len(plot_data) > 1:
            try:
                # Calculate trendline
                z = np.polyfit(plot_data["image_count"], plot_data["rating_number"], 1)
                p = np.poly1d(z)
                
                # Get x range for trendline (use fewer points)
                x_min = plot_data["image_count"].min()
                x_max = plot_data["image_count"].max()
                x_range = np.linspace(x_min, x_max, 20)  # Reduced number of points
                
                # Add trendline to plot
                fig.add_trace(
                    go.Scatter(
                        x=x_range,
                        y=p(x_range),
                        mode="lines",
                        name="Trend",
                        line=dict(color="#0d47a1", width=2, dash="dash"),
                        hoverinfo="skip"
                    )
                )
                
                # Calculate correlation
                correlation = np.corrcoef(plot_data["image_count"], plot_data["rating_number"])[0, 1]
                
                # Add correlation annotation
                fig.add_annotation(
                    text=f"Correlation: {correlation:.2f}",
                    xref="paper", yref="paper",
                    x=0.98, y=0.02,
                    showarrow=False,
                    font=dict(size=12, color="#333"),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="rgba(0,0,0,0.1)",
                    borderwidth=1,
                    borderpad=4,
                    align="right",
                    xanchor="right"
                )
            except Exception as e:
                print(f"Error calculating trendline: {e}")
    
    # Update layout
    fig.update_layout(
        title=None,
        xaxis_title="Number of Product Images",
        yaxis_title="Number of Reviews",
        font=dict(family="Inter, sans-serif"),
        plot_bgcolor="white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=400,
        showlegend=False
    )
    
    # Improve axis styling
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False,
        dtick=1  # Show every integer tick
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor="rgba(0,0,0,0.05)",
        zeroline=False
    )
    
    # Add insight annotation
    if not df_images_filtered.empty:
        # Find optimal image count (highest average rating)
        image_groups = df_images_filtered.groupby("image_count")["rating_number"].mean().reset_index()
        if not image_groups.empty:
            optimal_images = image_groups.loc[image_groups["rating_number"].idxmax(), "image_count"]
            
            fig.add_annotation(
                text=f"Optimal Image Count: {int(optimal_images)}",
                xref="paper", yref="paper",
                x=0.01, y=0.98,
                showarrow=False,
                font=dict(size=12, color="#333", weight="bold"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="#1976d2",
                borderwidth=1,
                borderpad=4
            )
    
    return fig


# --------------------------------------------
# 10) Run the Dash App
# --------------------------------------------
if __name__ == "__main__":
    app.run(debug=False)  # Set debug to False in production