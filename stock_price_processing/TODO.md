## Existing Dashboard
- [x] Display user name and email address
- [x] Fix x-axis labels on trade chart
- [x] Check whether filtering happens on the perspective level or on the client side
- [x] Add controls to add and remove tickers

## Analytics Dashboard
- [x] Display processed data
- [x] Show status of services and some information about the system in real time

## Data collector and aggregator server
- [x] Buffer in memory (processor + price-server SSE)
- [x] Dump as parquet files to blob storage (Hive `dt=` / `hour=` paths for DuckDB)

## Data aggregation batch job
- [ ] Process parquet files from blob storage
- [ ] Aggregate data into a single file
- [ ] Upload the aggregated file to blob storage
- [ ] Index should support using duckdb