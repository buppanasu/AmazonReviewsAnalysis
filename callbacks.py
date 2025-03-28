import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend to avoid GUI window creation
from dash import Input, Output
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import pandas as pd
from utils import empty_chart
import plotly.figure_factory as ff
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import io
import base64
import random
import math
import colorsys

def register_callbacks(app, cache, cat_monthly, df2, df_pivot, df_images_filtered, df_features_merged, df_desc, future_periods, df_avg_rating, df_category_rating, positive_words, negative_words, sentiment_counts, df_sentiment_by_year):
    """Register all callbacks for the dashboard"""
    
    # Update the sentiment counts chart callback to match your color scheme
    @app.callback(
        Output("sentiment-counts-chart", "figure"),
        [Input("sentiment-counts-loaded", "children")]
    )
    @cache.memoize()
    def update_sentiment_counts_chart(_):
        print("Callback triggered: update_sentiment_counts_chart")
        
        if not sentiment_counts:
            return empty_chart("No sentiment data available")
        
        try:
            # Extract sentiment types and counts
            sentiments = list(sentiment_counts.keys())
            counts = list(sentiment_counts.values())
            
            # Define colors for each sentiment to match your image
            colors = {
                "Positive": "green",  # Green
                "Negative": "red",    # Red
                "Neutral": "blue"     # Blue
            }
            
            # Create bar colors list
            bar_colors = [colors.get(sentiment, "#808080") for sentiment in sentiments]
            
            # Create bar chart
            fig = go.Figure()
            
            fig.add_trace(
                go.Bar(
                    x=sentiments,
                    y=counts,
                    marker_color=bar_colors,
                    text=counts,
                    textposition="auto",
                    hovertemplate="<b>%{x}</b><br>Count: %{y:,}<extra></extra>"
                )
            )
            
            # Calculate percentages for annotation
            total = sum(counts)
            percentages = [count / total * 100 for count in counts]
            
            # Update layout
            fig.update_layout(
                title="Overall Sentiment by Counts",
                xaxis_title="Sentiment",
                yaxis_title="Total Sentiment Count",
                font=dict(family="Inter, sans-serif"),
                plot_bgcolor="white",
                margin=dict(l=10, r=10, t=30, b=10),
                height=400,
                showlegend=False
            )
            
            # Improve axis styling
            fig.update_xaxes(
                showgrid=False,
                zeroline=False,
                categoryorder="array",
                categoryarray=["Positive", "Negative", "Neutral"]  # Ensure consistent order
            )
            
            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="rgba(0,0,0,0.05)",
                zeroline=False
            )
            
            # Add percentage annotations above each bar
            for i, (sentiment, count, percentage) in enumerate(zip(sentiments, counts, percentages)):
                fig.add_annotation(
                    x=sentiment,
                    y=count,
                    text=f"{percentage:.1f}%",
                    showarrow=False,
                    yshift=10,
                    font=dict(size=12, color="#333"),
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor=bar_colors[i],
                    borderwidth=1,
                    borderpad=3
                )
            
            # Add insight annotation
            max_sentiment = sentiments[counts.index(max(counts))]
            max_percentage = max(percentages)
            
            fig.add_annotation(
                text=f"Most common sentiment: {max_sentiment} ({max_percentage:.1f}%)",
                xref="paper", yref="paper",
                x=0.01, y=0.98,
                showarrow=False,
                font=dict(size=12, color="#333", weight="bold"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor=colors.get(max_sentiment, "#808080"),
                borderwidth=1,
                borderpad=4,
                align="left",
                xanchor="left"
            )
            
            return fig
        except Exception as e:
            print(f"Error generating sentiment counts chart: {e}")
            import traceback
            traceback.print_exc()
            return empty_chart(f"Error generating sentiment counts chart: {str(e)}")
    
    # New callback for Sentiment Analysis Word Cloud using a simpler approach
    @app.callback(
        Output("sentiment-wordcloud", "figure"),
        [Input("sentiment-dropdown", "value")]
    )
    @cache.memoize()
    def update_sentiment_wordcloud(selected_sentiment):
        print(f"Callback triggered: update_sentiment_wordcloud with sentiment: {selected_sentiment}")
        
        # Select the appropriate word dictionary based on sentiment
        word_dict = positive_words if selected_sentiment == "positive" else negative_words
        
        if not word_dict:
            return empty_chart("No sentiment data available")
        
        try:
            # Print some debug info
            print(f"Word dictionary has {len(word_dict)} items")
            print(f"Sample words: {list(word_dict.items())[:5]}")
            
            # Create a list of words and their frequencies
            words = list(word_dict.keys())
            frequencies = list(word_dict.values())
            
            # Sort words by frequency (descending)
            sorted_indices = sorted(range(len(frequencies)), key=lambda i: frequencies[i], reverse=True)
            sorted_words = [words[i] for i in sorted_indices]
            sorted_frequencies = [frequencies[i] for i in sorted_indices]
            
            # Limit to top 10 words only for better readability and less overlap
            max_words = min(10, len(sorted_words))
            words_to_show = sorted_words[:max_words]
            freqs_to_show = sorted_frequencies[:max_words]
            
            # Apply logarithmic scaling to make differences more apparent
            # This will make high-frequency words much larger than low-frequency ones
            max_freq = max(freqs_to_show) if freqs_to_show else 1
            min_freq = min(freqs_to_show) if freqs_to_show else 1
            
            # Use logarithmic scaling for more dramatic size differences
            if max_freq > min_freq * 5:  # Only use log scaling if there's significant difference
                # Add 1 to avoid log(1) = 0
                log_min = math.log(min_freq + 1)
                log_max = math.log(max_freq + 1)
                log_range = log_max - log_min
                
                # Scale between 20 and 100 for more dramatic size differences
                font_sizes = [20 + int(80 * (math.log(freq + 1) - log_min) / log_range) for freq in freqs_to_show]
            else:
                # Linear scaling for more uniform distributions
                freq_range = max_freq - min_freq
                if freq_range == 0:
                    freq_range = 1
                # Scale between 20 and 100 for more dramatic size differences
                font_sizes = [20 + int(80 * (freq - min_freq) / freq_range) for freq in freqs_to_show]
            
            # Make the top 3 words even larger to emphasize them
            if len(font_sizes) >= 3:
                font_sizes[0] = min(120, font_sizes[0] + 20)  # Top word
                if len(font_sizes) >= 2:
                    font_sizes[1] = min(100, font_sizes[1] + 15)  # Second word
                if len(font_sizes) >= 3:
                    font_sizes[2] = min(90, font_sizes[2] + 10)  # Third word
            
            # Define grid dimensions for word placement - larger grid for fewer words
            cols = 12  # Number of columns in the grid
            rows = 12  # Number of rows in the grid

            # Define colors based on sentiment
            if selected_sentiment == "positive":
                # Blue to green color palette
                colors = ['#1f77b4', '#2ca02c', '#3366cc', '#109618', '#0099c6', '#66aa00', '#3366cc']
            else:
                # Red to orange color palette
                colors = ['#d62728', '#ff7f0e', '#e31a1c', '#ff9900', '#dc3912', '#990099', '#ff4500']
            
            # Create a figure
            fig = go.Figure()
            
            # Fixed positions for top 10 words to avoid overlap
            # These positions are manually defined to ensure good spacing
            fixed_positions = [
                (cols/2, rows/2),      # Center - for the most frequent word
                (cols/4, rows/2),      # Left center
                (3*cols/4, rows/2),    # Right center
                (cols/2, rows/4),      # Top center
                (cols/2, 3*rows/4),    # Bottom center
                (cols/4, rows/4),      # Top left
                (3*cols/4, rows/4),    # Top right
                (cols/4, 3*rows/4),    # Bottom left
                (3*cols/4, 3*rows/4),  # Bottom right
                (cols/2, rows/3)       # Extra position
            ]
            
            # Add words as scatter points with text
            for i in range(min(max_words, len(fixed_positions))):
                word = words_to_show[i]
                freq = freqs_to_show[i]
                x, y = fixed_positions[i]
                
                # Choose color from the palette
                color_idx = i % len(colors)
                
                fig.add_trace(
                    go.Scatter(
                        x=[x],
                        y=[y],
                        mode="text",
                        text=[word],
                        textfont=dict(
                            size=font_sizes[i],
                            color=colors[color_idx]
                        ),
                        hoverinfo="text",
                        hovertext=f"{word}: {freq}",
                        showlegend=False
                    )
                )
            
            # Update layout
            fig.update_layout(
                title=None,
                showlegend=False,
                margin=dict(l=5, r=5, t=5, b=5),
                height=600,
                xaxis=dict(
                    showgrid=False,
                    showticklabels=False,
                    zeroline=False,
                    range=[0, cols]
                ),
                yaxis=dict(
                    showgrid=False,
                    showticklabels=False,
                    zeroline=False,
                    range=[rows, 0]  # Reverse y-axis to start from top
                ),
                plot_bgcolor="white",
                paper_bgcolor="white"
            )
            
            # Add a title annotation
            sentiment_title = "Positive Sentiment Words" if selected_sentiment == "positive" else "Negative Sentiment Words"
            fig.add_annotation(
                text=sentiment_title,
                xref="paper", yref="paper",
                x=0.5, y=1.05,
                showarrow=False,
                font=dict(
                    size=18, 
                    color="#333333",
                    weight="bold"
                ),
                align="center"
            )
            
            # Add insight annotation
            top_5_words = sorted(word_dict.items(), key=lambda x: x[1], reverse=True)[:5]
            top_words_text = ", ".join([f"{word}" for word, count in top_5_words])
            
            fig.add_annotation(
                text=f"Top words: {top_words_text}",
                xref="paper", yref="paper",
                x=0.5, y=-0.05,
                showarrow=False,
                font=dict(size=12, color="#333"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1,
                borderpad=4,
                align="center"
            )
            
            # Add frequency annotation for each word
            for i in range(min(max_words, len(fixed_positions))):
                word = words_to_show[i]
                freq = freqs_to_show[i]
                x, y = fixed_positions[i]
                
                # Add small annotation with frequency count
                fig.add_annotation(
                    x=x,
                    y=y,
                    text=f"({freq})",
                    showarrow=False,
                    yshift=-font_sizes[i]/2 - 10,  # Position below the word
                    font=dict(size=10, color="#666666"),
                    align="center"
                )
            
            return fig
        
        except Exception as e:
            print(f"Error generating word cloud: {e}")
            import traceback
            traceback.print_exc()
            return empty_chart(f"Error generating word cloud: {str(e)}")
    
    # Callback for Sentiment Distribution Over the Years
    @app.callback(
        Output("sentiment-by-year-chart", "figure"),
        [Input("sentiment-by-year-loaded", "children")]
    )
    @cache.memoize()
    def update_sentiment_by_year_chart(_):
        print("Callback triggered: update_sentiment_by_year_chart")
        
        if df_sentiment_by_year.empty:
            return empty_chart("No sentiment by year data available")
        
        try:
            # Create stacked bar chart for sentiment distribution over years
            fig = go.Figure()
            
            # Define colors for each sentiment
            colors = {
                "positive": "#2E8B57",  # Green
                "neutral": "#FF4500",   # Red-Orange
                "negative": "#FFC0CB"   # Pink
            }
            
            # Add trace for positive sentiment
            fig.add_trace(
                go.Bar(
                    x=df_sentiment_by_year["year"],
                    y=df_sentiment_by_year["positive"],
                    name="Positive",
                    marker_color=colors["positive"],
                    hovertemplate="<b>Year: %{x}</b><br>Positive: %{y:,}<extra></extra>"
                )
            )
            
            # Add trace for neutral sentiment
            fig.add_trace(
                go.Bar(
                    x=df_sentiment_by_year["year"],
                    y=df_sentiment_by_year["neutral"],
                    name="Neutral",
                    marker_color=colors["neutral"],
                    hovertemplate="<b>Year: %{x}</b><br>Neutral: %{y:,}<extra></extra>"
                )
            )
            
            # Add trace for negative sentiment
            fig.add_trace(
                go.Bar(
                    x=df_sentiment_by_year["year"],
                    y=df_sentiment_by_year["negative"],
                    name="Negative",
                    marker_color=colors["negative"],
                    hovertemplate="<b>Year: %{x}</b><br>Negative: %{y:,}<extra></extra>"
                )
            )
            
            # Update layout
            fig.update_layout(
                title=None,
                xaxis_title="Year",
                yaxis_title="Total Sentiment Count",
                font=dict(family="Inter, sans-serif"),
                plot_bgcolor="white",
                margin=dict(l=10, r=10, t=10, b=10),
                height=500,
                barmode="stack",
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=1.02,
                    xanchor="right",
                    x=1,
                    bgcolor="rgba(255,255,255,0.8)",
                    bordercolor="rgba(0,0,0,0.1)",
                    borderwidth=1
                ),
                hovermode="x unified"
            )
            
            # Improve axis styling
            fig.update_xaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="rgba(0,0,0,0.05)",
                zeroline=False,
                dtick=1,  # Show every year
                tickangle=45
            )
            
            fig.update_yaxes(
                showgrid=True,
                gridwidth=1,
                gridcolor="rgba(0,0,0,0.05)",
                zeroline=False,
                rangemode="tozero"
            )
            
            # Add insights
            # Find peak year
            total_by_year = df_sentiment_by_year["positive"] + df_sentiment_by_year["neutral"] + df_sentiment_by_year["negative"]
            peak_year_idx = total_by_year.idxmax()
            peak_year = df_sentiment_by_year.loc[peak_year_idx, "year"]
            peak_count = total_by_year[peak_year_idx]
            
            # Calculate sentiment ratios for the peak year
            positive_ratio = df_sentiment_by_year.loc[peak_year_idx, "positive"] / peak_count * 100
            neutral_ratio = df_sentiment_by_year.loc[peak_year_idx, "neutral"] / peak_count * 100
            negative_ratio = df_sentiment_by_year.loc[peak_year_idx, "negative"] / peak_count * 100
            
            # Add annotation for peak year
            fig.add_annotation(
                text=f"Peak Year: {peak_year} ({peak_count:,} reviews)<br>Positive: {positive_ratio:.1f}% | Neutral: {neutral_ratio:.1f}% | Negative: {negative_ratio:.1f}%",
                xref="paper", yref="paper",
                x=0.01, y=0.98,
                showarrow=False,
                font=dict(size=12, color="#333", weight="bold"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1,
                borderpad=4,
                align="left",
                xanchor="left"
            )
            
            # Find recent trend (last 5 years)
            recent_years = df_sentiment_by_year.tail(5)
            first_recent = recent_years.iloc[0]
            last_recent = recent_years.iloc[-1]
            
            trend_direction = "increasing" if total_by_year.iloc[-1] > total_by_year.iloc[-5] else "decreasing"
            trend_color = "#2E8B57" if trend_direction == "increasing" else "#B22222"
            
            # Add annotation for recent trend
            fig.add_annotation(
                text=f"Recent Trend: {trend_direction.capitalize()} ({first_recent['year']} to {last_recent['year']})",
                xref="paper", yref="paper",
                x=0.99, y=0.02,
                showarrow=False,
                font=dict(size=12, color="#333"),
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor=trend_color,
                borderwidth=1,
                borderpad=4,
                align="right",
                xanchor="right"
            )
            
            return fig
        except Exception as e:
            print(f"Error generating sentiment by year chart: {e}")
            import traceback
            traceback.print_exc()
            return empty_chart(f"Error generating sentiment by year chart: {str(e)}")
    
    # Rest of the callbacks remain unchanged
    @app.callback(
        Output("category-rating-graph", "figure"),
        [Input("shared-category-dropdown", "value")]
    )
    @cache.memoize()
    def update_category_rating_graph(selected_category):
        print(f"Callback triggered: update_category_rating_graph with category: {selected_category}")
        
        if selected_category is None:
            print("No category selected")
            return empty_chart("Please select a category")
        
        # Print dataframe info
        print(f"df_category_rating shape: {df_category_rating.shape}")
        print(f"df_category_rating columns: {df_category_rating.columns.tolist()}")
        print(f"df_category_rating unique categories: {df_category_rating['main_category'].unique().tolist()}")
        
        # Filter data for selected category
        category_data = df_category_rating[df_category_rating["main_category"] == selected_category].copy()
        print(f"Filtered data for {selected_category}: {len(category_data)} rows")
        
        if category_data.empty:
            print(f"No data available for {selected_category}")
            return empty_chart(f"No data available for {selected_category}")
        
        # Print sample of filtered data
        print("Sample of filtered data:")
        print(category_data.head())
        
        # Create line chart for category-specific rating over time
        fig = go.Figure()
        
        # Add trace for the rolling average
        fig.add_trace(
            go.Scatter(
                x=category_data["time_period"],
                y=category_data["rolling_avg"],
                mode="lines",
                name="12-Month Rolling Average",
                line=dict(color="#4361ee", width=3),
                hovertemplate="<b>%{x|%b %Y}</b><br>Rating: %{y:.2f}<extra></extra>"
            )
        )
        
        # Customize layout
        fig.update_layout(
            title=None,
            xaxis_title="Time Period",
            yaxis_title="Category Rating",
            font=dict(family="Inter, sans-serif"),
            plot_bgcolor="white",
            margin=dict(l=10, r=10, t=10, b=10),
            height=400,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1
            ),
            hovermode="x unified"
        )
        
        # Improve axis styling
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(0,0,0,0.05)",
            zeroline=False,
            tickangle=45,
            tickformat="%b %Y"
        )
        
        # Update y-axis to show full 0-5 range
        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(0,0,0,0.05)",
            zeroline=False,
            tickformat=".2f",
            range=[0, 5],  # Set fixed range from 0 to 5
            dtick=1  # Set tick interval to 1
        )
        
        # Add annotations for key insights
        min_rating = category_data["rolling_avg"].min()
        max_rating = category_data["rolling_avg"].max()
        min_date = category_data.loc[category_data["rolling_avg"].idxmin(), "time_period"]
        max_date = category_data.loc[category_data["rolling_avg"].idxmax(), "time_period"]
        
        # Add annotation for minimum point
        fig.add_annotation(
            x=min_date,
            y=min_rating,
            text=f"Min: {min_rating:.2f}",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#ff6b6b",
            ax=0,
            ay=30,
            font=dict(size=12, color="#333"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#ff6b6b",
            borderwidth=1,
            borderpad=4
        )
        
        # Add annotation for maximum point
        fig.add_annotation(
            x=max_date,
            y=max_rating,
            text=f"Max: {max_rating:.2f}",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#4361ee",
            ax=0,
            ay=-30,
            font=dict(size=12, color="#333"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#4361ee",
            borderwidth=1,
            borderpad=4
        )
        
        # Add overall trend annotation
        current_rating = category_data["rolling_avg"].iloc[-1]
        first_rating = category_data["rolling_avg"].iloc[0]
        trend_direction = "up" if current_rating > first_rating else "down"
        trend_color = "#2e8b57" if trend_direction == "up" else "#ff6b6b"
        trend_change = abs(current_rating - first_rating)
        
        fig.add_annotation(
            text=f"Overall Trend: {trend_direction.capitalize()} by {trend_change:.2f} stars",
            xref="paper", yref="paper",
            x=0.01, y=0.01,
            showarrow=False,
            font=dict(size=12, color="#333", weight="bold"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor=trend_color,
            borderwidth=1,
            borderpad=4,
            align="left",
            xanchor="left",
            yanchor="bottom"
        )
        
        print("Figure created successfully")
        return fig
    
    # New callback for Average Rating Over Time
    @app.callback(
        Output("average-rating-graph", "figure"),
        [Input("average-rating-data-loaded", "children")]
    )
    @cache.memoize()
    def update_average_rating_graph(_):
        if df_avg_rating.empty:
            return empty_chart("No average rating data available")
        
        # Create line chart for average rating over time
        fig = go.Figure()
        
        # Add trace for the rolling average
        fig.add_trace(
            go.Scatter(
                x=df_avg_rating["time_period"],
                y=df_avg_rating["rolling_avg"],
                mode="lines",
                name="12-Month Rolling Average",
                line=dict(color="#4361ee", width=3),
                hovertemplate="<b>%{x|%b %Y}</b><br>Rating: %{y:.2f}<extra></extra>"
            )
        )
        
        # Add trace for the actual average rating (lighter color)
        fig.add_trace(
            go.Scatter(
                x=df_avg_rating["time_period"],
                y=df_avg_rating["average_rating"],
                mode="lines",
                name="Monthly Average",
                line=dict(color="#a8c0ff", width=1.5),
                opacity=0.6,
                hovertemplate="<b>%{x|%b %Y}</b><br>Rating: %{y:.2f}<extra></extra>"
            )
        )
        
        # Customize layout
        fig.update_layout(
            title=None,
            xaxis_title="Time Period",
            yaxis_title="Average Rating",
            font=dict(family="Inter, sans-serif"),
            plot_bgcolor="white",
            margin=dict(l=10, r=10, t=10, b=10),
            height=400,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1,
                bgcolor="rgba(255,255,255,0.8)",
                bordercolor="rgba(0,0,0,0.1)",
                borderwidth=1
            ),
            hovermode="x unified"
        )
        
        # Improve axis styling
        fig.update_xaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(0,0,0,0.05)",
            zeroline=False,
            tickangle=45,
            tickformat="%b %Y"
        )
        
        # Update y-axis to show full 0-5 range
        fig.update_yaxes(
            showgrid=True,
            gridwidth=1,
            gridcolor="rgba(0,0,0,0.05)",
            zeroline=False,
            tickformat=".2f",
            range=[0, 5],  # Set fixed range from 0 to 5
            dtick=1  # Set tick interval to 1
        )
        
        # Add annotations for key insights
        min_rating = df_avg_rating["rolling_avg"].min()
        max_rating = df_avg_rating["rolling_avg"].max()
        min_date = df_avg_rating.loc[df_avg_rating["rolling_avg"].idxmin(), "time_period"]
        max_date = df_avg_rating.loc[df_avg_rating["rolling_avg"].idxmax(), "time_period"]
        
        # Add annotation for minimum point
        fig.add_annotation(
            x=min_date,
            y=min_rating,
            text=f"Min: {min_rating:.2f}",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#ff6b6b",
            ax=0,
            ay=30,
            font=dict(size=12, color="#333"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#ff6b6b",
            borderwidth=1,
            borderpad=4
        )
        
        # Add annotation for maximum point
        fig.add_annotation(
            x=max_date,
            y=max_rating,
            text=f"Max: {max_rating:.2f}",
            showarrow=True,
            arrowhead=2,
            arrowsize=1,
            arrowwidth=2,
            arrowcolor="#4361ee",
            ax=0,
            ay=-30,
            font=dict(size=12, color="#333"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#4361ee",
            borderwidth=1,
            borderpad=4
        )
        
        # Add overall trend annotation
        current_rating = df_avg_rating["rolling_avg"].iloc[-1]
        first_rating = df_avg_rating["rolling_avg"].iloc[0]
        trend_direction = "up" if current_rating > first_rating else "down"
        trend_color = "#2e8b57" if trend_direction == "up" else "#ff6b6b"
        trend_change = abs(current_rating - first_rating)
        
        fig.add_annotation(
            text=f"Overall Trend: {trend_direction.capitalize()} by {trend_change:.2f} stars",
            xref="paper", yref="paper",
            x=0.01, y=0.01,
            showarrow=False,
            font=dict(size=12, color="#333", weight="bold"),
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor=trend_color,
            borderwidth=1,
            borderpad=4,
            align="left",
            xanchor="left",
            yanchor="bottom"
        )
        
        return fig
    
    @app.callback(
        Output("graph-1", "figure"),
        [Input("shared-category-dropdown", "value")]
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

    # New callback for the "ALL" category visualization
    @app.callback(
        Output("graph-2-all", "figure"),
        [Input("all-category-value", "children")]
    )
    @cache.memoize()
    def update_graph_2_all(all_category):
        # Always use the "ALL" category
        selected_category = "ALL"

        # Filter data for ALL category
        group_data = df2[df2["category"] == selected_category].copy()
        top5 = group_data.sort_values(by="rating", ascending=False).head(5)

        if top5.empty:
            return empty_chart("No data available for ALL category")

        # Truncate long product titles and add line breaks for better display
        max_title_length = 50  # Maximum characters to display
        top5["display_title"] = top5["product_title"].apply(
            lambda x: '<br>'.join([x[i:i+max_title_length] for i in range(0, len(x), max_title_length)])
        )

        # Create color gradient for bars
        colors = px.colors.sequential.Blues[3:8]
        
        # Create bar chart for top 5 products with improved styling
        trace = go.Bar(
            y=top5["display_title"],
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
            margin=dict(l=20, r=10, t=10, b=10),  # Increased left margin
            height=500,  # Increased height to accommodate wrapped text
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
            automargin=True,  # This helps with long labels
        )
        
        # Add value labels to the bars
        for i, value in enumerate(top5["rating"]):
            fig.add_annotation(
                x=value,
                y=top5["display_title"].iloc[i],
                text=f"{value}",
                showarrow=False,
                xshift=5,
                font=dict(color="white" if value > max(top5["rating"]) * 0.3 else "#333"),
                xanchor="left"
            )

        return fig

    @app.callback(
        Output("graph-2", "figure"),
        [Input("shared-category-dropdown", "value")]
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

        # Truncate long product titles and add line breaks for better display
        max_title_length = 50  # Maximum characters to display
        top5["display_title"] = top5["product_title"].apply(
            lambda x: '<br>'.join([x[i:i+max_title_length] for i in range(0, len(x), max_title_length)])
        )

        # Create color gradient for bars
        colors = px.colors.sequential.Blues[3:8]
        
        # Create bar chart for top 5 products with improved styling
        trace = go.Bar(
            y=top5["display_title"],
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
            margin=dict(l=20, r=10, t=10, b=10),  # Increased left margin
            height=500,  # Increased height to accommodate wrapped text
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
            automargin=True,  # This helps with long labels
        )
        
        # Add a subtle annotation for the category
        fig.add_annotation(
            text=f"Category: {selected_category}",
            xref="paper", 
            yref="paper",
            x=0.01, 
            y=0.99,
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
                y=top5["display_title"].iloc[i],
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


    # Use a single callback for all product setting graphs to reduce overhead
    @app.callback(
        [Output("feature-count-graph", "figure"),
         Output("description-count-graph", "figure"),
         Output("image-count-graph", "figure")],
        [Input("product-settings-data-loaded", "children")]
    )
    @cache.memoize()
    def update_product_settings_graphs(_):
        # Create all three figures at once to reduce overhead
        feature_fig = create_feature_count_graph(df_features_merged)
        desc_fig = create_description_count_graph(df_desc)
        image_fig = create_image_count_graph(df_images_filtered)
        
        return feature_fig, desc_fig, image_fig


def create_feature_count_graph(df_features_merged):
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


def create_description_count_graph(df_desc):
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
        df_desc_copy = df_desc.copy()
        df_desc_copy['desc_range'] = (df_desc_copy['description_count'] // 100) * 100
        desc_groups = df_desc_copy.groupby("desc_range")["rating_number"].mean().reset_index()
        
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


def create_image_count_graph(df_images_filtered):
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
        zeroline=False,
        rangemode="tozero"
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

