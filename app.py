import warnings
from dash import Dash, html, dcc
from flask_caching import Cache

# Import components from our refactored modules
from layout import create_layout
from callbacks import register_callbacks
from data_loader import (
    load_rolling_avg_data,
    load_top_engagement_data,
    load_review_trends_data,
    load_product_settings_data,
    load_average_rating_data,
    load_category_rating_data,
    load_sentiment_analysis_data,
    load_sentiment_by_year_data
)

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Create Dash Application with Caching
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

# Load data at startup to avoid loading during callbacks
cat_monthly, categories1 = load_rolling_avg_data(cache)
df2, categories2 = load_top_engagement_data(cache)
df_pivot = load_review_trends_data(cache)
df_images_filtered, df_features_merged, df_desc = load_product_settings_data(cache)
df_avg_rating = load_average_rating_data(cache)
df_category_rating, category_rating_categories = load_category_rating_data(cache)
positive_words, negative_words, sentiment_counts = load_sentiment_analysis_data(cache)  # Updated to include sentiment_counts
df_sentiment_by_year = load_sentiment_by_year_data(cache)

# Forecast periods - reduced from 240 to 120 to decrease computation
future_periods = 120

# Set up the app layout
app.layout = create_layout(categories1, categories2, category_rating_categories)

# Set up the custom index string with CSS
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

# Register all callbacks
register_callbacks(
  app, 
  cache, 
  cat_monthly, 
  df2, 
  df_pivot, 
  df_images_filtered, 
  df_features_merged, 
  df_desc,
  future_periods,
  df_avg_rating,
  df_category_rating,
  positive_words,
  negative_words,
  sentiment_counts,
  df_sentiment_by_year
)

# Run the Dash App
if __name__ == "__main__":
  app.run(debug=False)  # Set debug to False in production
