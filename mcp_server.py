from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Any
import matplotlib.pyplot as plt
import os

from database.db import connect_to_mongo
from dal.repositories import AssetRepository, TimeSeriesRepository, AnalyticsRepository

mcp = FastMCP("Acme_Financial_Data_Warehouse")

connect_to_mongo()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "analytics", "Chat-Generated Outputs")

@mcp.tool()
def list_available_assets() -> List[Dict[str, Any]]:
    """Returns a list of all active financial assets currently stored in the data warehouse. This is used to find the exact 'asset_id' before running other tools."""
    try:
        repo = AssetRepository()
        assets = repo.findAll()
        return [
            {"asset_id": a["asset_id"], "symbol": a.get("symbol"), "asset_class": a.get("asset_class")} 
            for a in assets
        ]
    except Exception as e:
        return [{"error": f"DAL Error: {str(e)}"}]

@mcp.tool()
def get_asset_analytics(asset_id: str, limit_months: int = 12) -> List[Dict[str, Any]]:
    """Retrieves pre-calculated monthly analytics (high, low, average, count) for a specific asset. You MUST provide a valid 'asset_id' from the list_available_assets tool."""
    try:
        repo = AnalyticsRepository()
        results = repo.get_monthly_rollups(asset_id, limit_months)
        if not results:
            return [{"error": f"No analytics found for asset '{asset_id}'. Run the aggregator pipeline."}]
        return results
    except Exception as e:
        return [{"error": f"DAL Error: {str(e)}"}]

@mcp.tool()
def fetch_recent_time_series(asset_id: str, limit: int = 30) -> List[Dict[str, Any]]:
    """Fetches the raw, recent time-series data points (prices, indicators) for a specific asset.Useful for analyzing recent daily trends."""
    try:
        repo = TimeSeriesRepository()
        
        records = repo.findAll(asset_id, limit)
        
        formatted = []
        for r in records:
            formatted.append({
                "business_date": r["business_date"].isoformat() if "business_date" in r else None,
                "indicators": r.get("indicators", {}),
                "quality_flags": r.get("quality_flags", 0)
            })
        return formatted
    except Exception as e:
        return [{"error": f"DAL Error: {str(e)}"}]

@mcp.tool()
def generate_comparative_chart(asset_id_1: str, asset_id_2: str, limit_days: int = 90) -> str:
    """[Visual Tool] Generates a comparative line chart showing normalized percentage growth."""
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        repo = TimeSeriesRepository()
        
        data1 = repo.findAll(asset_id_1, limit_days)
        data2 = repo.findAll(asset_id_2, limit_days)
        
        if not data1 or not data2:
            return f"Error: Insufficient data to compare {asset_id_1} and {asset_id_2}."
            
        df1 = pd.DataFrame([{"date": d["business_date"].strftime("%Y-%m-%d"), "price1": d["indicators"]["close"]} for d in data1])
        df2 = pd.DataFrame([{"date": d["business_date"].strftime("%Y-%m-%d"), "price2": d["indicators"]["close"]} for d in data2])
        
        df = pd.merge(df1, df2, on="date").sort_values("date")
        if df.empty: return "Error: No overlapping trading dates found."
             
        df['growth1'] = (df['price1'] / df['price1'].iloc[0] - 1) * 100
        df['growth2'] = (df['price2'] / df['price2'].iloc[0] - 1) * 100
        
        plt.figure(figsize=(10, 5))
        plt.plot(df['date'], df['growth1'], label=asset_id_1.upper(), linewidth=2)
        plt.plot(df['date'], df['growth2'], label=asset_id_2.upper(), linewidth=2)
        
        plt.title(f"Comparative Growth: {asset_id_1.upper()} vs {asset_id_2.upper()}", fontsize=14)
        plt.ylabel("Return (%)")
        plt.axhline(0, color='black', linewidth=1, linestyle='--')
        plt.xticks(df['date'][::len(df)//10], rotation=45) 
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        
        # --- PATH FIX ---
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = f"compare_{asset_id_1}_vs_{asset_id_2}.png"
        filepath = os.path.join(OUTPUT_DIR, filename)
        # ----------------
        
        plt.savefig(filepath)
        plt.close()
        
        return f"SUCCESS: Comparative chart saved to: {filepath}"
    except Exception as e:
        return f"Failed to generate chart: {str(e)}"

@mcp.tool()
def generate_price_chart(asset_id: str, limit_months: int = 6) -> str:
    """[Visual Tool] Generates a professional line chart of the monthly average prices for an asset and saves it to the analytics output folder."""
    try:
        import matplotlib.pyplot as plt
        repo = AnalyticsRepository()
        data = repo.get_monthly_rollups(asset_id, limit_months)
        
        if not data:
            return f"Error: No data found to chart for {asset_id}. Run the aggregator."

        chart_data = [d for d in data if d.get("period") is not None]
        if not chart_data:
            return f"Error: Analytics found, but no valid monthly periods exist to plot for {asset_id}."
            
        chart_data.reverse()

        periods = [d["period"] for d in chart_data]
        averages = [d["stats"]["average"] for d in chart_data]

        plt.figure(figsize=(10, 5))
        plt.plot(periods, averages, marker='o', linestyle='-', color='#2c3e50', linewidth=2)
        plt.title(f"Monthly Average Price - {asset_id.upper()}", fontsize=14)
        plt.xlabel("Month", fontsize=12)
        plt.ylabel("Price (USD)", fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        # --- PATH FIX ---
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = f"{asset_id}_performance_chart.png"
        filepath = os.path.join(OUTPUT_DIR, filename)
        # ----------------
        
        plt.savefig(filepath)
        plt.close()

        return f"SUCCESS: I have generated the chart and saved it to: {filepath}"
    except Exception as e:
        return f"Failed to generate chart: {str(e)}"

@mcp.tool()
def generate_volume_momentum_chart(asset_id: str, limit_days: int = 60) -> str:
    """[Visual Tool] Generates a dual-axis chart showing Price (line) and Volume (bars)and saves it to the analytics output folder."""
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        repo = TimeSeriesRepository()
        
        data = repo.findAll(asset_id, limit_days)
        if not data: return f"Error: No data for {asset_id}."
        
        df = pd.DataFrame([{
            "date": d["business_date"], 
            "close": d["indicators"]["close"],
            "volume": d["indicators"]["volume"]
        } for d in data]).sort_values("date")

        fig, ax1 = plt.subplots(figsize=(10, 5))

        color = 'tab:red'
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Price (USD)', color=color)
        ax1.plot(df['date'], df['close'], color=color, linewidth=2)
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()
        color = 'tab:blue'
        ax2.set_ylabel('Volume', color=color)
        ax2.bar(df['date'], df['volume'], color=color, alpha=0.3)
        ax2.tick_params(axis='y', labelcolor=color)

        plt.title(f"Price vs Volume Momentum - {asset_id.upper()}")
        fig.tight_layout()
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = f"volume_{asset_id}.png"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        plt.savefig(filepath)
        plt.close()
        
        return f"SUCCESS: Volume chart saved to: {filepath}"
    except Exception as e:
        return f"Failed to generate volume chart: {str(e)}"

@mcp.tool()
def get_database_coverage_report() -> dict:
    """[Quantitative Tool] Returns a summary of how much historical data is stored for each asset, including the oldest date, newest date, and total years of coverage.
    This is called when asked about the length of history, data limits, or amount of data stored."""
    try:
        from dal.repositories import TimeSeriesRepository
        repo = TimeSeriesRepository()
        summary = repo.get_coverage_summary()
        
        if not summary:
            return {"error": "No time series data found in the database."}
        
        report = {}
        for item in summary:
            asset = item["_id"]
            oldest = item["oldest_record"]
            newest = item["newest_record"]
            count = item["total_records"]
            
            days_diff = (newest - oldest).days
            years = round(days_diff / 365.25, 2)
            
            report[asset] = {
                "oldest_date": oldest.strftime("%Y-%m-%d"),
                "newest_date": newest.strftime("%Y-%m-%d"),
                "total_trading_days_stored": count,
                "years_of_history": years
            }
        
        return report
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def analyze_asset_correlation(asset_id_1: str, asset_id_2: str, limit_days: int = 365) -> dict:
    """[Quantitative Tool] Calculates the Pearson correlation coefficient between two assets."""
    try:
        import pandas as pd
        from dal.repositories import TimeSeriesRepository
        repo = TimeSeriesRepository()
        
        data1 = repo.findAll(asset_id_1, limit_days)
        data2 = repo.findAll(asset_id_2, limit_days)
        
        if not data1 or not data2: return {"error": "Insufficient data."}
            
        df1 = pd.DataFrame([{"date": d["business_date"].strftime("%Y-%m-%d"), "price1": d["indicators"]["close"]} for d in data1])
        df2 = pd.DataFrame([{"date": d["business_date"].strftime("%Y-%m-%d"), "price2": d["indicators"]["close"]} for d in data2])
        
        df = pd.merge(df1, df2, on="date").sort_values("date")
        if df.empty: return {"error": "No overlapping dates after standardizing timestamps."}
        
        df['return1'] = df['price1'].pct_change()
        df['return2'] = df['price2'].pct_change()
        
        correlation = df['return1'].corr(df['return2'])
        
        interpretation = "Highly Correlated" if correlation > 0.7 else \
                         "Moderately Correlated" if correlation > 0.3 else \
                         "Uncorrelated" if correlation > -0.3 else \
                         "Negatively Correlated"
                         
        return {
            "asset_1": asset_id_1,
            "asset_2": asset_id_2,
            "overlapping_trading_days": len(df),
            "correlation_coefficient": round(correlation, 4),
            "interpretation": interpretation
        }
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def analyze_rsi(asset_id: str, periods: int = 14) -> dict:
    """[Quantitative Tool] Calculates the 14-day Relative Strength Index (RSI).This is used to determine if an asset is technically Overbought (>70) or Oversold (<30)."""
    try:
        import pandas as pd
        from dal.repositories import TimeSeriesRepository
        repo = TimeSeriesRepository()
        data = repo.findAll(asset_id, limit=periods * 3) 
        if len(data) < periods + 1: return {"error": "Not enough data for RSI."}
        
        df = pd.DataFrame([{"date": d["business_date"], "close": d["indicators"]["close"]} for d in data]).sort_values("date")
        
        delta = df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=periods).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=periods).mean()
        
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        
        latest_rsi = df['RSI'].iloc[-1]
        
        condition = "Overbought (Bearish Signal)" if latest_rsi >= 70 else \
                    "Oversold (Bullish Signal)" if latest_rsi <= 30 else \
                    "Neutral"
                    
        return {
            "asset_id": asset_id,
            "current_rsi": round(latest_rsi, 2),
            "market_condition": condition,
            "insight": "Values above 70 indicate the asset may be overvalued. Values below 30 indicate it may be undervalued."
        }
    except Exception as e:
        return {"error": str(e)}

@mcp.tool()
def generate_return_distribution_chart(asset_id: str, limit_days: int = 365) -> str:
    """[Visual Tool] Generates a histogram showing the distribution of daily returns (risk profile). Call this when asked about risk distribution or daily swings."""
    try:
        import pandas as pd
        import matplotlib.pyplot as plt
        from dal.repositories import TimeSeriesRepository
        repo = TimeSeriesRepository()
        
        data = repo.findAll(asset_id, limit_days)
        if not data: return f"Error: No data for {asset_id}."
        
        df = pd.DataFrame([{"close": d["indicators"]["close"]} for d in data]).iloc[::-1]
        df['daily_return'] = df['close'].pct_change() * 100
        df = df.dropna()

        plt.figure(figsize=(10, 5))
        plt.hist(df['daily_return'], bins=50, color='#3498db', edgecolor='black', alpha=0.7)
        
        plt.axvline(0, color='red', linestyle='dashed', linewidth=2)
        
        plt.title(f"Daily Return Distribution (Risk Profile) - {asset_id.upper()}", fontsize=14)
        plt.xlabel("Daily Return (%)", fontsize=12)
        plt.ylabel("Frequency (Days)", fontsize=12)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.tight_layout()
        
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        filename = f"distribution_{asset_id}.png"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        plt.savefig(filepath)
        plt.close()
        
        return f"SUCCESS: Distribution histogram saved to: {filepath}"
    except Exception as e:
        return f"Failed to generate histogram: {str(e)}"
    
@mcp.tool()
def get_ml_predictions(asset_id: str, limit: int = 5) -> dict:
    """[Quantitative Tool] Retrieves the latest Machine Learning predictions generated by the Apache Spark GBT model for a SPECIFIC asset. Call this when asked what the model predicts for tomorrow's price."""
    try:
        from database.db import get_db
        db = get_db()
        collection = db["spark_regression_results"]
        
        cursor = collection.find({"asset_id": asset_id}).sort("bdate", -1).limit(limit)
        
        import datetime
        
        today_date = datetime.date.today() 
        
        formatted_results = []
        for doc in cursor:
            bdate_raw = doc["bdate"]
            if hasattr(bdate_raw, 'date'):
                target_date = bdate_raw.date()
            elif isinstance(bdate_raw, datetime.datetime):
                target_date = bdate_raw.date()
            else:
                target_date = datetime.datetime.strptime(str(bdate_raw)[:10], "%Y-%m-%d").date()
            
            if target_date == today_date:
                timeline_label = "Today's Open (Current Context)"
            elif target_date == today_date + datetime.timedelta(days=1):
                timeline_label = "Tomorrow's Forecast"
            elif target_date < today_date:
                timeline_label = "Historical Backtest Prediction"
            else:
                timeline_label = "Future Forecast"

            formatted_results.append({
                "target_date": target_date.strftime("%Y-%m-%d"),
                "timeline_context": timeline_label,
                "predicted_return_percentage": round(doc.get("prediction", 0) * 100, 2),
                "predicted_price_usd": round(doc.get("predicted_price_usd", 0), 2)
            })
            
        if not formatted_results:
            return {"error": f"No Spark predictions found for {asset_id}. Please run the spark_ml_regression.py pipeline for this specific asset first."}
            
        return {
            "asset_id": asset_id,
            "insight": "These are the predictions from the Apache Spark Gradient Boosted Trees model.",
            "predictions": formatted_results
        }
    except Exception as e:
        return {"error": f"Failed to retrieve predictions: {str(e)}"}

if __name__ == "__main__":
    print("Starting Acme Data Warehouse MCP Server...")
    mcp.run(transport='stdio')