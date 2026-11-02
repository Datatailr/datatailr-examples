# Merit Desk

Merit Desk is a small power trading desk. It turns a weather forecast into a price curve for three power markets: Great Britain (GB), Germany (DE) and France (FR). A forward curve lists the price of power for each of the next 12 months. Power prices follow the weather. A cold day raises demand, and the grid then runs more expensive power plants. The desk models this with a merit order: the plants run from the cheapest to the most expensive, and the last plant that must run sets the price. The temperatures come from ECMWF, a European weather centre. Demand and prices come from this merit order model, so the desk uses no real market prices.

## Layout

```text
merit_desk/
├── run_local.py            # Prices the three markets on your laptop
├── requirements*.txt       # The packages that the code needs, and pytest for the tests
├── common/                 # The model: stack.py, markets.py, weather.py, curve.py and more
└── tests/
```

## Run it on your laptop

Use Python 3.10 or newer. Run these commands from the repository root:

```bash
pip install -r merit_desk/requirements-dev.txt
python merit_desk/run_local.py
```

The script downloads about 21 MB of forecast from ECMWF. If the download fails, it uses the sample forecast in `merit_desk/common/sample_data/`. It saves its files in `.merit_desk_data/`. It prints the front-month price of each market. The front month is the first month of the curve. For example:

```text
Merit Desk curve 2026-11-02 (forecast: ECMWF IFS HRES open data, run 2026-11-02 00z)
GB 2026-11: 154.83 EUR/MWh
DE 2026-11: 170.00 EUR/MWh
FR 2026-11: 152.32 EUR/MWh
```

## Data

> Temperature forecasts: based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF), licensed under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Modified: daily mean 2 m temperature at one grid point per city.

© 2026 European Centre for Medium-Range Weather Forecasts (ECMWF). ECMWF terms of use: https://apps.ecmwf.int/datasets/licences/general/

ECMWF gives this disclaimer:

> "ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use."

Ask your coding assistant to deploy this.
