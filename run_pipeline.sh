set -e
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${BLUE}=======================================================${NC}"
echo -e "${GREEN}Acme Ltd. Financial Data Warehouse - Orchestration Script${NC}"
echo -e "${BLUE}=======================================================${NC}\n"

echo -e "${YELLOW}[1/5] Executing Isolated Unit Test Suite (PyTest)...${NC}"
pytest tests/
echo -e "${GREEN}[SUCCESS] Unit test suite passed.${NC}\n"

echo -e "${YELLOW}[2/5] Executing Ingestion Pipeline & Schema Initialization...${NC}"
python -m ingestion.etl
echo -e "${GREEN}[SUCCESS] Data ingestion and schema initialization complete.${NC}\n"

echo -e "${YELLOW}[3/5] Computing Native MongoDB Data Mining Aggregations...${NC}"
python -m analytics.aggregator
echo -e "${GREEN}[SUCCESS] Native MongoDB aggregations complete.${NC}\n"

echo -e "${YELLOW}[4/5] Executing Apache Spark Aggregation Workflow...${NC}"
python spark_analytics/spark_aggregation.py
echo -e "${GREEN}[SUCCESS] Distributed Spark aggregations complete and persisted.${NC}\n"

echo -e "${YELLOW}[5/5] Executing Apache Spark Machine Learning Pipeline...${NC}"
python spark_analytics/spark_ml_regression.py
echo -e "${GREEN}[SUCCESS] Machine learning predictions generated and stored.${NC}\n"

echo -e "${BLUE}=======================================================${NC}"
echo -e "${GREEN}ALL BATCH PROCESSES CONCLUDED SUCCESSFULLY${NC}"
echo -e "${BLUE}=======================================================${NC}\n"

echo -e "The Data Warehouse is fully populated and ready for consumption."
echo -e "To initialize the live serving layers, execute the following commands in separate terminal sessions:\n"

echo -e "  ${YELLOW}1. Initialize the REST API:${NC}"
echo -e "     uvicorn api.main:app --reload\n"

echo -e "  ${YELLOW}2. Initialize the AI Agent (MCP Server):${NC}"
echo -e "     (Ensure mcp_server.py is registered in your respective AI client configuration)\n"