import plotly.graph_objects as go

def empty_chart(message="No data available"):
    """Helper function for empty charts"""
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