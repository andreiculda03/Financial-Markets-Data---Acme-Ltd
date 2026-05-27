# Acme Ltd. Financial Data Warehouse

## Project Summary
This repository contains the end-to-end implementation of a bi-temporal, production-grade financial data warehouse. The system is engineered to ingest, normalize, and persist high-frequency market data across multiple asset classes (equities, indices, and cryptocurrencies). The architecture features a robust Data Access Layer (DAL) ensuring temporal correctness, distributed analytical pipelines powered by Apache Spark, a RESTful consumption API, and an advanced Model Context Protocol (MCP) server that enables local Large Language Models (LLMs) to perform grounded quantitative analysis directly against the warehouse infrastructure.

---

## Architectural Overview
The architecture enforces strict separation of concerns across five primary layers:

1. **Ingestion Layer (`ingestion/`)**: Implements a Provider-Agnostic Factory Pattern for dynamic traffic routing (Alpha Vantage / Yahoo Finance). It ensures data pipeline idempotency through a rigorous "Filter and Insert" pattern, preventing data duplication during concurrent ETL operations on native time-series collections.
2. **Persistence & Data Access Layer (`database/` & `dal/`)**: Utilizes MongoDB with Native Time-Series collections for optimized storage implementing a Slowly Changing Dimension (SCD) Type 2 semantic for the Asset catalog, thus ensuring non-destructive historical versioning.
3. **Distributed Compute Layer (`spark_analytics/`)**: Executes cross-asset machine learning pipelines utilizing Gradient Boosted Trees to engineer stationary returns, perform chronological validation, and persist forward-looking pricing predictions.
4. **API Consumption Layer (`api/`)**: A FastAPI-driven REST interface exposing strictly paginated and temporally accurate endpoints.
5. **Agentic AI Integration (`mcp_server.py`)**: An MCP-compliant server exposing 11 grounded quantitative tools (e.g., Pearson correlation, RSI, prediction retrieval) to localized LLM agents.

---

## Part 1: Environment Configuration and Prerequisites

To evaluate this pipeline, the host environment must be configured with Python 3.10+, Java (for Apache Spark JVM instantiation), and a local instance of MongoDB.

### 1.1. MongoDB Initialization
Ensure a local MongoDB instance is running on the default port.
* **URI:** `mongodb://127.0.0.1:27017/`
* *Note: Manual schema creation is not required. The application layer handles database instantiation, collection configuration, and index generation dynamically upon first execution.*

### 1.2. Virtual Environment Configuration
It is strictly required to execute this project within an isolated Python virtual environment to prevent dependency conflicts.

```bash
# 1. Instantiate the virtual environment
python3 -m venv venv

# 2. Activate the virtual environment
# On macOS/Linux:
source venv/bin/activate
# On Windows:
# venv\Scripts\activate

# 3. Install project dependencies
pip install -r requirements.txt

```

### 1.3. Environment Variables

Construct a `.env` file in the root directory of the project. This file securely provisions the database connection and third-party API configurations. Populate it with the following parameters:

```env
MONGODB_URL="mongodb://127.0.0.1:27017/"
DATABASE_NAME="acme_ltd_db"
ALPHA_VANTAGE_API_KEY="your_api_key_here" 

```

---

## Part 2: System Evaluation and Automated Orchestration

To systematically evaluate the batch processes of this architecture, manual execution of individual modules is unnecessary. A master orchestration script (`run_pipeline.sh`) is provided to automate the build, test, and ingestion processes sequentially.

### 2.1. Granting Execution Permissions (macOS/Linux)

Prior to initial execution, the shell script must be granted executable permissions by the host operating system:

```bash
chmod +x run_pipeline.sh

```

### 2.2. Executing the Master Pipeline

Run the orchestration script from the root directory:

```bash
./run_pipeline.sh

```

**Execution Sequence:**

1. **Isolated Unit Testing (`pytest`):** Validates temporal logic (SCD Type 2) and data normalization transformations in a mocked environment.
2. **Data Ingestion and Schema Initialization (`etl.py`):** Dynamically provisions MongoDB schemas, establishes native time-series structures, and executes the multi-provider ETL extraction and load processes.
3. **Data Mining Aggregations (`aggregator.py`):** Executes native MongoDB aggregation pipelines to calculate monthly rollups.
4. **Spark Distributed Aggregation (`spark_aggregation.py`):** Validates distributed compute capabilities by calculating parallelized frequency metrics via Apache Spark.
5. **Spark Machine Learning (`spark_ml_regression.py`):** Instantiates the JVM, calculates stationary features across the 20-asset portfolio, trains the GBT regression model, and persists predictions to the database.

---

## Part 3: Live Serving Layers (Consumption)

Upon the successful execution of the orchestration script, the data warehouse is fully populated. The live serving layers must be instantiated in separate, active terminal sessions.

### 3.1. Instantiating the RESTful API

To query the database via traditional HTTP requests, start the FastAPI server:

```bash
uvicorn api.main:app --reload

```

* **Documentation:** Once initialized, navigate to `http://127.0.0.1:8000/docs` to access the interactive Swagger UI and explore the paginated endpoints.

### 3.2. Instantiating the Model Context Protocol (MCP) Server

To evaluate the Agentic AI integration, the MCP server must be registered with a compatible LLM client (e.g., Claude Desktop, LM Studio).

1. Ensure the absolute path to `mcp_server.py` is accurately configured in your client's configuration JSON.
2. The server utilizes `stdio` transport. It will initialize silently and await JSON-RPC packets from the host LLM.
3. **Evaluation Prompt Example:** *“Please analyze AAPL and MSFT. Provide their database coverage, their correlation coefficient, and tomorrow's Spark GBT predictions. Finally, generate a comparative growth chart for the two.”*

---

## Testing Methodology

The system enforces continuous integration principles via `pytest` and `unittest.mock`.

* `test_dal.py` validates repository-level idempotency and Slowly Changing Dimension state changes.
* `test_etl.py` validates extraction integrity and provider-agnostic data normalization structures.
