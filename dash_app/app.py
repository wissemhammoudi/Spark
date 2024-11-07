import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import dash_bootstrap_components as dbc
import plotly.graph_objs as go
import pandas as pd
import sqlalchemy
from dash_bootstrap_templates import load_figure_template

# Load the dark theme for the entire app
load_figure_template("DARKLY")

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])

app.layout = html.Div(
    [
        dbc.NavbarSimple(
            brand="Weather Station Analysis",
            brand_href="#",
            color="dark",
            dark=True,
        ),
        dbc.Container(
            [
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(id="station-1-gauge"), width=4),
                        dbc.Col(dcc.Graph(id="station-2-gauge"), width=4),
                        dbc.Col(dcc.Graph(id="station-3-gauge"), width=4),
                    ],
                    align="center",
                ),
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(id="station-4-gauge"), width=4),
                        dbc.Col(dcc.Graph(id="station-5-gauge"), width=4),
                    ],
                    align="center",
                ),
                dbc.Row([dbc.Col(dcc.Graph(id="temperature-trends"), width=12)]),
                dbc.Row(
                    [
                        dbc.Col(dcc.Graph(id="humidity-wind"), width=6),
                        dbc.Col(dcc.Graph(id="precipitation"), width=6),
                    ]
                ),
                dcc.Interval(
                    id="interval-component", interval=10 * 1000, n_intervals=0
                ),
            ],
            fluid=True,
        ),
    ]
)


@app.callback(
    [
        Output("station-1-gauge", "figure"),
        Output("station-2-gauge", "figure"),
        Output("station-3-gauge", "figure"),
        Output("station-4-gauge", "figure"),
        Output("station-5-gauge", "figure"),
        Output("temperature-trends", "figure"),
        Output("humidity-wind", "figure"),
        Output("precipitation", "figure"),
    ],
    Input("interval-component", "n_intervals"),
)
def update_metrics(n):
    # Create database connection
    engine = sqlalchemy.create_engine("postgresql://admin:admin@postgres:5432/weather")

    # Using context managers for handling database connections
    with engine.connect() as conn:
        df = pd.read_sql(
            """
            SELECT * FROM weather_metrics 
            WHERE end_time = (SELECT MAX(end_time) FROM weather_metrics)
            """, 
            conn
        )

        historical_data = pd.read_sql(
            """
            SELECT city_id, city_name, end_time, avg_temperature
            FROM weather_metrics
            ORDER BY city_id, end_time
            LIMIT 300;
            """,
            conn,
        )

    # Create gauges for the latest temperatures
    gauges = []
    for i in range(1, 6):
        station_data = df[df["city_id"] == f"city_{i}"]
        temperature = station_data["avg_temperature"].values[0] if not station_data.empty else 0
        station_name = station_data["city_name"].values[0] if not station_data.empty else f"Station {i}"
        gauge = go.Figure(
            go.Indicator(
                mode="gauge+number",
                value=temperature,
                domain={"x": [0, 1], "y": [0, 1]},
                title={"text": f"{station_name} Temperature", "align": "center"},
                gauge={"axis": {"range": [-20, 50]}},
            )
        )
        gauge.update_layout(template="plotly_dark")
        gauges.append(gauge)
    # Temperature Trends Plot
    temp_trends = go.Figure()
    for i in range(1, 6):  # Loop through all 5 stations
        city_data = historical_data[historical_data["city_id"] == f"city_{i}"]
        if not city_data.empty:
            city_name = city_data["city_name"].iloc[0]
            temp_trends.add_trace(
                go.Scatter(
                    x=city_data["end_time"],
                    y=city_data["avg_temperature"],
                    mode="lines+markers",
                    name=f"{city_name}",
                )
            )

    temp_trends.update_layout(
        title="Temperature Trends Over Time",
        xaxis_title="Time", 
        yaxis_title="Temperature (°C)",
        legend_title="Station",
        template="plotly_dark",
    )

    # Humidity and Wind Speed Plot
    humidity_wind = go.Figure()
    humidity_wind.add_trace(
        go.Bar(
            x=df["city_name"],
            y=df["avg_humidity"],
            name="Humidity (%)",
        )
    )
    humidity_wind.add_trace(
        go.Bar(
            x=df["city_name"],
            y=df["avg_wind_speed"],
            name="Wind Speed (m/s)",
        )
    )
    humidity_wind.update_layout(
        barmode="group",
        title="Humidity and Wind Speed by Station",
        xaxis_title="Station",
        yaxis_title="Value",
        template="plotly_dark",
    )

    # Precipitation Plot
    precipitation = go.Figure(
        go.Bar(
            x=df["city_name"],
            y=df["avg_precipitation"],
            text=df["avg_precipitation"],
            textposition="auto",
        )
    )
    precipitation.update_layout(
        title="Average Precipitation by Station",
        xaxis_title="Station",
        yaxis_title="Precipitation (mm)",
        template="plotly_dark",
    )

    return gauges + [temp_trends, humidity_wind, precipitation]


if __name__ == "__main__":
    app.run_server(debug=False, host="0.0.0.0")
