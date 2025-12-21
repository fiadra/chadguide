"""
Flight Intelligence Dashboard.

A Streamlit-based interactive dashboard for visualizing and analyzing
flight data collected by the flight scanner. Provides price analysis,
comfort metrics, route mapping, and detailed data exploration.

Usage:
    streamlit run dashboard.py
"""

import logging
import sqlite3
from datetime import date
from typing import List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Configure logging for console output (no emojis)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ==========================================================================
# UI CONFIGURATION
# ==========================================================================

st.set_page_config(
    page_title="Flight Intelligence Dashboard",
    page_icon="✈️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS styling
st.markdown("""
<style>
    .metric-card {
        background-color: #f8f9fa;
        border: 1px solid #e9ecef;
        border-radius: 10px;
        padding: 20px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    div[data-testid="stMetricValue"] {
        font-size: 28px;
        font-weight: bold;
        color: #0f172a;
    }
    div[data-testid="stMetricLabel"] {
        font-size: 14px;
        color: #64748b;
    }
</style>
""", unsafe_allow_html=True)


# ==========================================================================
# DATA LOADING AND PROCESSING
# ==========================================================================

@st.cache_data
def load_data() -> pd.DataFrame:
    """
    Load and preprocess flight data from the SQLite database.

    Performs a JOIN between flight quotes and static flight information,
    applies data type conversions, and calculates derived metrics like
    comfort score.

    Returns:
        A pandas DataFrame containing the merged flight data with
        additional computed columns. Returns an empty DataFrame if
        the database is unavailable or empty.
    """
    db_path: str = "Duffel_api/flights.db"

    try:
        logger.info("Loading data from database: %s", db_path)
        conn: sqlite3.Connection = sqlite3.connect(db_path)

        # SQL JOIN: Combine quotes with flight details
        query: str = """
        SELECT
            q.price_amount, q.currency, q.departure_date, q.fare_brand,
            q.baggage_checked, q.baggage_carryon,
            s.*
        FROM flight_quotes q
        LEFT JOIN flights_static s ON q.flight_static_id = s.route_id
        """
        df: pd.DataFrame = pd.read_sql(query, conn)
        conn.close()

        if df.empty:
            logger.warning("Database returned no records")
            return pd.DataFrame()

        # Data type conversions
        df['departure_date'] = pd.to_datetime(df['departure_date'])
        df['day_of_week'] = df['departure_date'].dt.day_name()
        df['day_num'] = df['departure_date'].dt.weekday  # 0=Monday

        df['has_wifi'] = df['has_wifi'].astype(bool)
        df['has_power'] = df['has_power'].astype(bool)
        df['co2_kg'] = pd.to_numeric(df['co2_kg'], errors='coerce').fillna(0)

        # Parse seat pitch (extract numeric value from text)
        df['seat_pitch_num'] = pd.to_numeric(
            df['seat_pitch'].astype(str).str.extract(r'(\d+)')[0],
            errors='coerce'
        ).fillna(29)

        # Calculate comfort score (0-4 points)
        df['comfort_score'] = (
            (df['seat_pitch_num'] >= 30).astype(int) * 2 +
            df['has_wifi'].astype(int) +
            df['has_power'].astype(int)
        )

        logger.info("Loaded %d records from database", len(df))
        return df

    except Exception as error:
        logger.exception("Database error: %s", error)
        st.error(f"Database error: {error}")
        return pd.DataFrame()


def apply_filters(
    df: pd.DataFrame,
    origin: str,
    destinations: List[str],
    airlines: List[str],
    max_price: int,
    date_range: Tuple[date, date],
    require_wifi: bool,
    require_baggage: bool
) -> pd.DataFrame:
    """
    Apply user-selected filters to the flight data.

    Args:
        df: The complete flight DataFrame.
        origin: Selected origin airport IATA code.
        destinations: List of selected destination IATA codes.
        airlines: List of selected airline names.
        max_price: Maximum price filter value.
        date_range: Tuple of (start_date, end_date) for filtering.
        require_wifi: If True, only include flights with WiFi.
        require_baggage: If True, only include flights with checked baggage.

    Returns:
        Filtered DataFrame matching all criteria.
    """
    mask = (
        (df['origin_iata'] == origin) &
        (df['dest_iata'].isin(destinations)) &
        (df['carrier_name'].isin(airlines)) &
        (df['price_amount'] <= max_price) &
        (df['departure_date'].dt.date >= date_range[0]) &
        (df['departure_date'].dt.date <= date_range[1])
    )

    if require_wifi:
        mask &= df['has_wifi']
    if require_baggage:
        mask &= (df['baggage_checked'] > 0)

    return df[mask]


# ==========================================================================
# CHART RENDERING FUNCTIONS
# ==========================================================================

def render_price_by_day_chart(df: pd.DataFrame) -> None:
    """
    Render a bar chart showing average prices by day of week.

    Args:
        df: Filtered flight DataFrame.
    """
    if df.empty:
        return

    daily_avg = df.groupby(
        ['day_of_week', 'day_num']
    )['price_amount'].mean().reset_index()
    daily_avg = daily_avg.sort_values('day_num')

    fig = px.bar(
        daily_avg,
        x='day_of_week',
        y='price_amount',
        text_auto='.0f',
        color='price_amount',
        color_continuous_scale='RdYlGn_r',
        labels={'price_amount': 'Average price', 'day_of_week': 'Day'},
        height=400
    )
    st.plotly_chart(fig, use_container_width=True)


def render_price_distribution_chart(df: pd.DataFrame) -> None:
    """
    Render a box plot showing price distribution by airline.

    Args:
        df: Filtered flight DataFrame.
    """
    if df.empty:
        return

    fig = px.box(
        df,
        x='carrier_code',
        y='price_amount',
        color='carrier_code',
        points="all",
        height=400,
        title="Price stability"
    )
    st.plotly_chart(fig, use_container_width=True)


def render_price_calendar_chart(df: pd.DataFrame) -> None:
    """
    Render a line chart showing minimum daily prices over time.

    Args:
        df: Filtered flight DataFrame.
    """
    if df.empty:
        return

    daily_trend = df.groupby('departure_date')['price_amount'].min().reset_index()
    fig = px.line(
        daily_trend,
        x='departure_date',
        y='price_amount',
        markers=True,
        title="Lowest available price per day"
    )
    st.plotly_chart(fig, use_container_width=True)


def render_value_matrix_chart(df: pd.DataFrame) -> None:
    """
    Render a scatter plot showing price vs comfort score.

    Args:
        df: Filtered flight DataFrame.
    """
    if df.empty:
        return

    fig = px.scatter(
        df,
        x="price_amount",
        y="comfort_score",
        color="carrier_name",
        size="seat_pitch_num",
        hover_data=['aircraft_model', 'dest_iata'],
        labels={
            "price_amount": "Price",
            "comfort_score": "Comfort Score (0-4)"
        },
    )
    st.plotly_chart(fig, use_container_width=True)


def render_eco_chart(df: pd.DataFrame) -> None:
    """
    Render a horizontal bar chart showing routes with lowest CO2 emissions.

    Args:
        df: Filtered flight DataFrame.
    """
    if df.empty:
        return

    eco_df = df.groupby(
        ['dest_iata', 'carrier_name']
    )['co2_kg'].mean().reset_index().sort_values('co2_kg')

    fig = px.bar(
        eco_df.head(10),
        x='co2_kg',
        y='carrier_name',
        orientation='h',
        color='co2_kg',
        color_continuous_scale='Teal',
        text_auto='.1f',
        title="Top 10 eco-friendly routes (lowest kg CO2)"
    )
    st.plotly_chart(fig, use_container_width=True)


def render_route_map(df: pd.DataFrame) -> None:
    """
    Render an interactive map showing flight routes from origin.

    Args:
        df: Filtered flight DataFrame.
    """
    if df.empty:
        return

    # Aggregate to unique routes
    routes_geo = df.drop_duplicates(
        subset=['origin_iata', 'dest_iata', 'carrier_code']
    )

    fig = go.Figure()

    # Draw route lines
    for _, row in routes_geo.iterrows():
        fig.add_trace(go.Scattergeo(
            lon=[row['origin_lon'], row['dest_lon']],
            lat=[row['origin_lat'], row['dest_lat']],
            mode='lines',
            line=dict(width=1, color='#3b82f6'),
            opacity=0.5,
            hoverinfo='none'
        ))

    # Draw destination markers (size based on offer count)
    dest_counts = df['dest_iata'].value_counts()

    for dest_code in routes_geo['dest_iata'].unique():
        dest_data = routes_geo[routes_geo['dest_iata'] == dest_code].iloc[0]
        count = dest_counts[dest_code]

        fig.add_trace(go.Scattergeo(
            lon=[dest_data['dest_lon']],
            lat=[dest_data['dest_lat']],
            mode='markers',
            marker=dict(
                size=8 + (count / 5),
                color='#ef4444',
                symbol='circle'
            ),
            name=dest_code,
            text=f"{dest_code}: {count} offers",
            hoverinfo='text'
        ))

    # Draw origin marker
    origin_row = routes_geo.iloc[0]
    fig.add_trace(go.Scattergeo(
        lon=[origin_row['origin_lon']],
        lat=[origin_row['origin_lat']],
        mode='markers',
        marker=dict(size=15, color='#10b981', symbol='star'),
        name=origin_row['origin_iata']
    ))

    fig.update_layout(
        geo=dict(
            scope='europe',
            projection_type='azimuthal equal area',
            showland=True,
            landcolor='#f3f4f6',
            countrycolor='#e5e7eb',
        ),
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=600
    )
    st.plotly_chart(fig, use_container_width=True)


# ==========================================================================
# MAIN APPLICATION
# ==========================================================================

def main() -> None:
    """
    Main application entry point.

    Loads data, renders sidebar filters, and displays the main dashboard
    with multiple analysis tabs.
    """
    df = load_data()

    # Sidebar filters
    st.sidebar.title("🔍 Filters")

    if df.empty:
        st.error("Database is empty. Run 'run_scanner.py' first!")
        st.stop()

    # Origin selection
    origins: List[str] = df['origin_iata'].unique().tolist()
    selected_origin: str = st.sidebar.selectbox("Departure from:", origins)

    # Destination selection (filtered by origin)
    available_destinations: List[str] = df[
        df['origin_iata'] == selected_origin
    ]['dest_iata'].unique().tolist()
    selected_destinations: List[str] = st.sidebar.multiselect(
        "Arrival to:",
        available_destinations,
        default=available_destinations
    )

    # Date range filter
    min_date: date = df['departure_date'].min().date()
    max_date: date = df['departure_date'].max().date()
    selected_dates = st.sidebar.date_input(
        "Date range:",
        [min_date, max_date],
        min_value=min_date,
        max_value=max_date
    )

    # Ensure we have a valid date range tuple
    if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
        date_range: Tuple[date, date] = selected_dates
    else:
        date_range = (min_date, max_date)

    # Airline filter
    selected_airlines: List[str] = st.sidebar.multiselect(
        "Airlines:",
        df['carrier_name'].unique().tolist(),
        default=df['carrier_name'].unique().tolist()
    )

    # Price filter
    max_price_available: int = int(df['price_amount'].max()) if not df.empty else 1000
    selected_max_price: int = st.sidebar.slider(
        "Maximum price (EUR):",
        0,
        max_price_available,
        max_price_available
    )

    # Comfort filters
    require_wifi: bool = st.sidebar.checkbox("WiFi required 📶")
    require_baggage: bool = st.sidebar.checkbox("Checked baggage included 🧳")

    # Apply filters
    df_filtered = apply_filters(
        df,
        selected_origin,
        selected_destinations,
        selected_airlines,
        selected_max_price,
        date_range,
        require_wifi,
        require_baggage
    )

    # Main dashboard header
    st.title(f"✈️ Price Analysis: {selected_origin} → Europe")
    st.markdown(f"Analyzing **{len(df_filtered)}** flight offers in the selected period.")

    # KPI cards
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if not df_filtered.empty:
            best_price: float = df_filtered['price_amount'].min()
            currency: Optional[str] = df_filtered.iloc[0]['currency']
            st.metric("Lowest Price", f"{best_price:.2f} {currency}")
        else:
            st.metric("Lowest Price", "-")

    with col2:
        if not df_filtered.empty:
            avg_price: float = df_filtered['price_amount'].mean()
            currency = df_filtered.iloc[0]['currency']
            st.metric("Average Price", f"{avg_price:.0f} {currency}")
        else:
            st.metric("Average Price", "-")

    with col3:
        unique_routes: int = df_filtered['dest_iata'].nunique()
        st.metric("Available Destinations", f"{unique_routes}")

    with col4:
        if not df_filtered.empty:
            cheap_flights = df_filtered[df_filtered['price_amount'] < avg_price]
            if not cheap_flights.empty:
                top_value_airline: str = cheap_flights['carrier_name'].mode()[0]
                st.metric("Best Value Airline", top_value_airline)
            else:
                st.metric("Best Value Airline", "-")
        else:
            st.metric("Best Value Airline", "-")

    st.markdown("---")

    # Analysis tabs
    tab_prices, tab_quality, tab_map, tab_raw = st.tabs([
        "💰 Price Analysis (Trends)",
        "⭐ Quality & Comfort",
        "🌍 Route Map",
        "📋 Detailed Data"
    ])

    # Tab 1: Prices
    with tab_prices:
        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("📅 Best Days to Fly (Average price by day)")
            render_price_by_day_chart(df_filtered)

        with col2:
            st.subheader("📊 Price Distribution by Airline")
            render_price_distribution_chart(df_filtered)

        st.subheader("📈 Price Calendar (Time Series)")
        render_price_calendar_chart(df_filtered)

    # Tab 2: Quality
    with tab_quality:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("💎 Value for Money Matrix")
            st.caption("Look for points in bottom-right (high comfort, low price)")
            render_value_matrix_chart(df_filtered)

        with col2:
            st.subheader("🌱 Carbon Footprint (Most Eco Routes)")
            render_eco_chart(df_filtered)

    # Tab 3: Map
    with tab_map:
        if not df_filtered.empty:
            st.subheader("Route network from selected airport")
            render_route_map(df_filtered)

    # Tab 4: Raw data
    with tab_raw:
        st.subheader("Detailed offer list")

        columns_to_display: List[str] = [
            'departure_date', 'carrier_name', 'flight_number',
            'origin_iata', 'dest_iata', 'price_amount', 'currency',
            'aircraft_model', 'has_wifi', 'baggage_checked', 'co2_kg'
        ]

        st.dataframe(
            df_filtered[columns_to_display],
            column_config={
                "logo_url": st.column_config.ImageColumn("Logo"),
                "departure_date": st.column_config.DateColumn("Date"),
                "price_amount": st.column_config.NumberColumn(
                    "Price",
                    format="%.2f"
                ),
                "has_wifi": st.column_config.CheckboxColumn("WiFi"),
                "baggage_checked": st.column_config.NumberColumn(
                    "Baggage",
                    help="Number of checked bags"
                ),
                "co2_kg": st.column_config.ProgressColumn(
                    "CO2",
                    min_value=0,
                    max_value=300,
                    format="%.0f kg"
                ),
            },
            use_container_width=True,
            height=600
        )


if __name__ == "__main__":
    main()
else:
    # When run via streamlit run, __name__ is not "__main__"
    main()
