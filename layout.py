from dash import html, dcc

def create_layout(categories1, categories2, category_rating_categories):
  """Create the dashboard layout"""
  # Safely get default values for dropdowns
  default_category1 = categories1[0] if isinstance(categories1, list) and len(categories1) > 0 else None
  
  # Filter categories for specific visualizations (exclude 'ALL' category)
  specific_categories = [cat for cat in categories2 if cat != 'ALL']
  default_specific_category = specific_categories[0] if specific_categories else None
  
  # Default category for rating by category visualization
  default_category_rating = category_rating_categories[0] if category_rating_categories else None
  
  return html.Div(
      className="dashboard-container",
      children=[
          # Header
          html.Div(
              className="header",
              children=[
                  html.H1("Product Analytics Dashboard"),
                  html.P("Analyze product performance, reviews, and settings")
              ]
          ),
          
          # General Visualizations Section Header
          html.H2("General Visualizations", className="section-header"),
          
          # Overall Sentiment Analysis Bar Chart
          html.Div(
              className="card",
              children=[
                  html.Div(
                      className="card-header",
                      children=[
                          html.H3("Overall Sentiment Analysis", className="card-title"),
                          # Hidden div for storing data load state
                          html.Div(id="sentiment-counts-loaded", style={"display": "none"}, children="loaded")
                      ]
                  ),
                  html.Div(
                      className="graph-container",
                      children=[
                          dcc.Graph(
                              id="sentiment-counts-chart",
                              config={"displayModeBar": False}
                          )
                      ]
                  )
              ]
          ),
          
          # Sentiment Distribution Over the Years
          html.Div(
              className="card",
              children=[
                  html.Div(
                      className="card-header",
                      children=[
                          html.H3("Sentiment Distribution Over the Years", className="card-title"),
                          # Hidden div for storing data load state
                          html.Div(id="sentiment-by-year-loaded", style={"display": "none"}, children="loaded")
                      ]
                  ),
                  html.Div(
                      className="graph-container",
                      children=[
                          dcc.Graph(
                              id="sentiment-by-year-chart",
                              config={"displayModeBar": False}
                          )
                      ]
                  )
              ]
          ),
          
          # Sentiment Analysis Word Cloud
          html.Div(
              className="card",
              children=[
                  html.Div(
                      className="card-header",
                      children=[
                          html.H3("Sentiment Analysis Word Cloud", className="card-title"),
                          html.Div(
                              className="dropdown-container",
                              children=[
                                  dcc.Dropdown(
                                      id="sentiment-dropdown",
                                      options=[
                                          {"label": "Positive Sentiment", "value": "positive"},
                                          {"label": "Negative Sentiment", "value": "negative"}
                                      ],
                                      value="positive",
                                      clearable=False
                                  )
                              ]
                          ),
                          # Hidden div for storing data load state
                          html.Div(id="sentiment-data-loaded", style={"display": "none"}, children="loaded")
                      ]
                  ),
                  html.Div(
                      className="graph-container",
                      children=[
                          dcc.Graph(
                              id="sentiment-wordcloud",
                              config={"displayModeBar": False}
                          )
                      ]
                  )
              ]
          ),
          
          # Average Rating Over Time
          html.Div(
              className="card",
              children=[
                  html.Div(
                      className="card-header",
                      children=[
                          html.H3("Average Rating Over Time", className="card-title"),
                          # Hidden div for storing data load state
                          html.Div(id="average-rating-data-loaded", style={"display": "none"}, children="loaded")
                      ]
                  ),
                  html.Div(
                      className="graph-container",
                      children=[
                          dcc.Graph(
                              id="average-rating-graph",
                              config={"displayModeBar": False}
                          )
                      ]
                  )
              ]
          ),
          
          # Top Products by Rating (ALL category only)
          html.Div(
              className="card",
              children=[
                  html.Div(
                      className="card-header",
                      children=[
                          html.H3("Top Products by Rating (All Categories)", className="card-title"),
                          # Hidden div to store the ALL category value
                          html.Div(id="all-category-value", style={"display": "none"}, children="ALL")
                      ]
                  ),
                  html.Div(
                      className="graph-container",
                      children=[
                          dcc.Graph(
                              id="graph-2-all",
                              config={"displayModeBar": False}
                          )
                      ]
                  )
              ]
          ),
          
          # Card 3: Review Trends
          html.Div(
              className="card",
              children=[
                  html.Div(
                      className="card-header",
                      children=[
                          html.H3("Review Trends Over Time", className="card-title"),
                          html.Div(
                              className="dropdown-container",
                              children=[
                                  dcc.Dropdown(
                                      id="review-dropdown",
                                      options=[
                                          {"label": "All Reviews", "value": "overall"},
                                          {"label": "Five Star Reviews", "value": "five_star"},
                                          {"label": "One Star Reviews", "value": "one_star"},
                                          {"label": "Combined View", "value": "combined"}
                                      ],
                                      value="overall",
                                      clearable=False
                                  )
                              ]
                          )
                      ]
                  ),
                  html.Div(
                      className="graph-container",
                      children=[
                          dcc.Graph(
                              id="review-graph",
                              config={"displayModeBar": False}
                          )
                      ]
                  )
              ]
          ),
          
          # Card 4: Product Settings Analysis
          html.Div(
              className="card",
              children=[
                  html.Div(
                      className="card-header",
                      children=[
                          html.H3("Product Settings Analysis", className="card-title"),
                          # Hidden div for storing data load state
                          html.Div(id="product-settings-data-loaded", style={"display": "none"}, children="loaded")
                      ]
                  ),
                  html.Div(
                      style={"display": "flex", "flexWrap": "wrap", "gap": "20px"},
                      children=[
                          # Feature Count vs. Rating
                          html.Div(
                              style={"flex": "1", "minWidth": "300px"},
                              children=[
                                  html.H4("Feature Count vs. Reviews", style={"fontSize": "14px", "margin": "10px 0"}),
                                  dcc.Graph(
                                      id="feature-count-graph",
                                      config={"displayModeBar": False}
                                  )
                              ]
                          ),
                          # Description Length vs. Rating
                          html.Div(
                              style={"flex": "1", "minWidth": "300px"},
                              children=[
                                  html.H4("Description Bullet Point Count vs. Reviews", style={"fontSize": "14px", "margin": "10px 0"}),
                                  dcc.Graph(
                                      id="description-count-graph",
                                      config={"displayModeBar": False}
                                  )
                              ]
                          ),
                          # Image Count vs. Rating
                          html.Div(
                              style={"flex": "1", "minWidth": "300px"},
                              children=[
                                  html.H4("Image Count vs. Reviews", style={"fontSize": "14px", "margin": "10px 0"}),
                                  dcc.Graph(
                                      id="image-count-graph",
                                      config={"displayModeBar": False}
                                  )
                              ]
                          )
                      ]
                  )
              ]
          ),
          
          # Specific Visualizations Section Header
          html.H2("Specific Visualizations", className="section-header"),
          
          # Add a single shared dropdown for all specific visualizations
          html.Div(
              className="card",
              style={"marginBottom": "20px", "padding": "10px"},
              children=[
                  html.Div(
                      style={"display": "flex", "alignItems": "center", "justifyContent": "space-between"},
                      children=[
                          html.H3("Category Filter", style={"margin": "0", "fontSize": "16px", "fontWeight": "600"}),
                          html.Div(
                              style={"width": "300px"},
                              children=[
                                  dcc.Dropdown(
                                      id="shared-category-dropdown",
                                      options=[{"label": cat, "value": cat} for cat in categories2 if cat != 'ALL'],
                                      value=default_specific_category,
                                      clearable=False,
                                      placeholder="Select a category"
                                  )
                              ]
                          )
                      ]
                  )
              ]
          ),
        
        # Category-Specific Sentiment Word Cloud (NEW)
        html.Div(
            className="card",
            children=[
                html.Div(
                    className="card-header",
                    children=[
                        html.H3("Category Sentiment Word Cloud", className="card-title"),
                        html.Div(
                            className="dropdown-container",
                            children=[
                                dcc.Dropdown(
                                    id="category-sentiment-dropdown",
                                    options=[
                                        {"label": "Positive Sentiment", "value": "positive"},
                                        {"label": "Negative Sentiment", "value": "negative"}
                                    ],
                                    value="positive",
                                    clearable=False
                                )
                            ]
                        )
                    ]
                ),
                html.Div(
                    className="graph-container",
                    children=[
                        dcc.Graph(
                            id="category-sentiment-wordcloud",
                            config={"displayModeBar": False}
                        )
                    ]
                )
            ]
        ),
        
        # Average Rating Over Time by Category
        html.Div(
            className="card",
            children=[
                html.Div(
                    className="card-header",
                    children=[
                        html.H3("Average Rating Over Time by Category", className="card-title"),
                        # Remove the individual dropdown here
                    ]
                ),
                html.Div(
                    className="graph-container",
                    children=[
                        dcc.Graph(
                            id="category-rating-graph",
                            config={"displayModeBar": False}
                        )
                    ]
                )
            ]
        ),
          
          # First row of specific visualizations
          html.Div(
              className="flex-container",
              style={"display": "flex", "gap": "20px"},
              children=[
                  # Card: Category Rating Trends (moved from General)
                  html.Div(
                      className="card",
                      style={"width": "50%"},
                      children=[
                          html.Div(
                              className="card-header",
                              children=[
                                  html.H3("Category Rating Trends", className="card-title"),
                                  # Remove the individual dropdown here
                              ]
                          ),
                          html.Div(
                              className="graph-container",
                              children=[
                                  dcc.Graph(
                                      id="graph-1",
                                      config={"displayModeBar": False}
                                  )
                              ]
                          )
                      ]
                  ),
                  
                  # Card: Top Products by Rating (with category filter)
                  html.Div(
                      className="card",
                      style={"width": "50%"},
                      children=[
                          html.Div(
                              className="card-header",
                              children=[
                                  html.H3("Top Products by Category", className="card-title"),
                                  # Remove the individual dropdown here
                              ]
                          ),
                          html.Div(
                              className="graph-container",
                              style={"height": "500px"},  # Increase the height to accommodate wrapped text
                              children=[
                                  dcc.Graph(
                                      id="graph-2",
                                      config={"displayModeBar": False}
                                  )
                              ]
                          )
                      ]
                  )
              ]
          ),
          
          # Footer
          html.Footer(
              className="footer",
              children=[
                  html.P("Product Analytics Dashboard © 2023")
              ]
          )
      ]
  )

