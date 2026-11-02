# Merit Desk on Datatailr

Merit Desk is a small power trading desk that runs on Datatailr. Every weekday morning it turns a weather forecast into a price curve for three power markets. Traders read the curves in a web app and in Excel. In the app, they also test a weather scenario, change a power plant or run a backtest. The app sends each of these calculations to Datatailr as a job.

The demo has seven Datatailr jobs: three workflows, two services, an app and an Excel add-in. A workflow is a set of tasks, and tasks that do not depend on each other run in parallel. A service is a program that runs all the time. An app is a web page. The app is built with FastAPI, a Python web framework. The jobs share data through blob storage, the file store of Datatailr. The app also creates a short-lived workflow for each calculation that a trader starts, and removes it about 10 minutes after the run ends.

## What the desk does

Power prices follow the weather. A cold day raises demand, and the grid then runs more expensive power plants. The desk models this with a merit order, also called a stack. In a merit order, plants run from the cheapest to the most expensive. The last plant that must run sets the price, which is called the clearing price.

Every weekday at 07:00 London time, the desk does four things:

1. It downloads a temperature forecast from ECMWF, a European weather centre.
2. It turns the temperatures into prices for Great Britain (GB), Germany (DE) and France (FR).
3. It saves one forward curve for each market. A forward curve lists the price of power for each of the next 12 months.
4. It posts a short summary to the desk chat.

Two more workflows run beside the daily curve. The strategy sweep backtests a weather rule on many combinations at once. A backtest tests a rule on past prices. The demand training teaches a small neural network to predict power demand from the temperature. It runs on a GPU, a graphics processor.

## The jobs

| Job | Kind | What it does |
| --- | --- | --- |
| Merit Desk Daily Curve | Workflow, weekdays 07:00 London | Downloads the forecast, prices each market in parallel, saves the curves, posts the summary |
| Merit Desk Strategy Sweep | Workflow, Sundays 02:00 London | Runs one backtest task for each combination of market, tenor and threshold, then ranks them |
| Merit Desk Demand Training | Workflow, no schedule | Trains a demand model on a GPU and saves it |
| Pricing API | Service, `http://pricing-api` | Gives the newest curve over HTTP, and the stack price for a temperature |
| Market Feed | Service, `http://market-feed` | Serves simulated live prices |
| Merit Desk | App (FastAPI) | Shows the curves, the live prices, the merit order and the newest sweep. Runs scenarios and backtests as jobs. Starts the Daily Curve and the Strategy Sweep |
| Merit Desk Run `<label>` `<6 characters>` | Workflow, one for each click, made by the app | Computes one scenario or one backtest and writes the result into its run record. The app removes the workflow about 10 minutes after the run ends |
| Desk Tools | Excel add-in | Gives the prices as Excel functions |

A tenor is how many months ahead a contract delivers. A threshold is how much colder than normal the forecast must be, in degrees Celsius, before the rule buys.

In the name of a run workflow, the label is a short name for the click, such as `Scenario DE -3.0C`. The run id is the id of the run record, a file in blob storage that holds the inputs, the status and the result of one run. A run id starts with the time.

The jobs fit together like this:

```text
ECMWF open data
     |  temperature forecast
     v
Daily Curve (weekdays 07:00) ------------> desk chat (summary)
     |  curves
     v
Blob storage <------ Strategy Sweep (Sundays 02:00)   writes the backtest results
     ^        <------ Demand Training (no schedule)   writes the trained model
     |        <------ Merit Desk Run (one per click)   writes its run record
     |
     +--> Pricing API --+--> Merit Desk (app) --> creates and removes the Merit Desk Run workflows
     +--> Market Feed --+--> Desk Tools (Excel)
```

The Merit Desk app also reads the newest forecast, the backtest results and the run records from blob storage. On the Jobs page, it starts the Daily Curve and the Strategy Sweep. Other jobs reach a service by its host name. The host name is the service name in lower case, with hyphens for spaces.

## The Merit Desk app

The app has five pages. You pick a page in the menu on the left. A bar at the top shows the live prices of GB, DE and FR, the environment and your name. The prices are simulated, and the bar says so. The runs rail on the right lists the runs that the app started, newest first. Each card shows the status, the time the run took, who started it and the name of the job.

| Page | What you can do |
| --- | --- |
| Curves | Chart the newest 12-month curve of each market, from the Pricing API. You can hide a market. This page only reads. |
| Scenario | Shift the temperature over a window of dates. The page redraws the shifted curve next to the base curve at once. "Run as job" prices the scenario over many simulated weather paths and adds a band from P10 to P90. |
| Merit order | See the plants of a market, cheapest first, with the demand at a temperature and the clearing price. Change the cost or the capacity of a plant, and the clearing price moves at once. "Price the 12-month curve with this stack" runs the job of the Scenario page with the changed plants. |
| Backtest | See the total P&L of the newest strategy sweep as a heatmap of one market, by tenor and threshold. A click on a cell fills the form. "Run backtest as job" backtests that one setting and shows its P&L, drawdown, hit rate and trades. |
| Jobs | Start the Daily Curve or the Strategy Sweep now. Each has a card with its schedule and its last run. The Strategy Sweep card also shows the size of the newest sweep, and the variable `sweep_size`, which applies at the next deploy. A table lists the runs that the app started, and who started them. |

A weather path is the forecast temperature plus random noise on each day of the curve. A scenario job prices many paths and keeps the average price of each month for each path. P10, P50 and P90 are the 10th, 50th and 90th percentile of those averages. P&L is profit and loss. The drawdown is how far the total P&L has fallen below its highest value so far. The hit rate is the share of trades that made money.

### Previews and runs

The app computes a preview at once, inside the app, and redraws the chart as you move an input. The Scenario page and the Merit order page have a preview. The Backtest page has none, because the newest sweep already gives a quick answer.

A click on "Run as job" sends the full calculation to Datatailr:

1. The app checks the inputs. If one is out of range, it answers with the reason.
2. It writes a run record to blob storage and shows a run card in the rail.
3. It builds a workflow with one task, deploys it and starts it. The workflow is named `Merit Desk Run <label> <6 characters>`, for example `Merit Desk Run Scenario DE -3.0C 0f2a9c`. The `<6 characters>` are the last six characters of the run id, so two identical clicks give two jobs.
4. The task computes the result and writes it into the run record. It gets only the run id. The inputs stay in the record.
5. The app sees that the run has ended and shows the result on the page without a reload. About 10 minutes later, it removes the workflow.

## Layout

```text
merit_desk/
├── deploy.py                 # Deploys one target, or all of them
├── run_local.py              # Runs the daily curve on your laptop
├── metadata.json             # The card in the template list
├── requirements-dev.txt      # Packages for local work, tests and the notebook workstation
├── admin/setup_demo.py       # One-time cluster setup, run by an administrator
├── common/                   # Code that all jobs share
│   ├── stack.py              # The merit-order price model
│   ├── markets.py            # GB, DE and FR: cities and power plants
│   ├── climate.py            # Normal temperature for each day
│   ├── dates.py              # Today's date in London
│   ├── weather.py            # ECMWF download, with a fallback
│   ├── curve.py              # Builds the forward curve
│   ├── scenario.py           # A temperature shift and changed plants, and the weather-path band
│   ├── store.py              # Where files live: blob storage, a local folder or memory
│   ├── runs.py               # Run records: one JSON file for each click
│   ├── messages.py           # Makes the platform's error text short and safe for the page
│   ├── settings.py           # Reads the variables "markets" and "sweep_size"
│   ├── notify.py             # Posts the summary to the chat
│   ├── history.py            # Made-up history for backtests and training
│   ├── backtest.py           # One backtest with its detail, and the grid of combinations to sweep
│   ├── demand_model.py       # Demand model: neural network and grid search
│   └── sample_data/          # A saved forecast, for when the download fails
├── workflows/                # daily_curve.py, strategy_sweep.py, demand_training.py,
│                             # desk_runs.py (the workflow of one click),
│                             # requirements.txt, training-requirements.txt (with torch)
├── services/                 # pricing_api.py, market_feed.py, requirements.txt
├── desk_app/                 # The Merit Desk app
│   ├── app.py                # The FastAPI app: the page, the JSON API and live updates (server-sent events)
│   ├── client.py             # Calls to the Pricing API and the Market Feed
│   ├── runner.py             # Runs a click as a Datatailr workflow, or in a thread
│   ├── manager.py            # Starts and watches the runs, and allows three at a time
│   ├── static/               # The page: HTML, CSS, JavaScript and Chart.js
│   └── requirements.txt
├── excel/                    # desk_tools.py, requirements.txt
├── notebooks/                # 01_instant_run.ipynb, 02_tune_demand_model.ipynb
└── tests/
```

All code lives under `merit_desk/`, because Datatailr ships only the top-level package of a job's entry point. Every job imports shared code with absolute imports, for example `from merit_desk.common.stack import stack_price`.

## Blob storage layout

Every file lives under `merit-desk/<environment>/`, where the environment is `dev`, `pre` or `prod`. The environments never share files.

```text
merit-desk/<environment>/
  forecasts/<run date>.json         # Each saved forecast
  curves/latest/<market>.json       # The newest curve of each market
  curves/<date>/<market>.json       # The curve of each day
  backtests/latest.json             # The newest sweep: all results, and the top 10
  runs/<run id>.json                # One record for each run that the app starts
  models/demand-DE.pt               # The trained demand model
```

## Run it on your laptop

You need no Datatailr login. Use Python 3.12, the version that the jobs use. Python 3.10 is the minimum. Run these commands from the repository root:

```bash
pip install -r merit_desk/requirements-dev.txt
python -m pytest merit_desk/tests -q
python merit_desk/run_local.py
```

`run_local.py` runs the steps of the daily curve on your machine. It downloads about 21 MB from ECMWF. If the download fails, it uses the newest forecast that it saved before in `.merit_desk_data/`. If there is none, it uses the sample forecast in `merit_desk/common/sample_data/`. It writes its files to `.merit_desk_data/` and prints the front-month price of each market. The front month is the first month of the curve. The script does not post to the chat.

To see the app, run `run_local.py` first, so that the Pricing API has a curve. Then run these commands from the repository root. They start the two services and the app:

```bash
export PRICING_API_URL=http://localhost:8001 MARKET_FEED_URL=http://localhost:8002
MERIT_DESK_STORE=local uvicorn merit_desk.services.pricing_api:create_app --factory --port 8001 &
MERIT_DESK_STORE=local uvicorn merit_desk.services.market_feed:create_app --factory --port 8002 &
MERIT_DESK_STORE=local MERIT_DESK_RUNNER=local uvicorn merit_desk.desk_app.app:app --port 8000
```

Then open http://localhost:8000. `MERIT_DESK_STORE=local` keeps the files in `.merit_desk_data/`, not in blob storage. `MERIT_DESK_RUNNER=local` runs each click in a thread of the app, not as a Datatailr job, so a run ends in a moment.

On a laptop, the Backtest page says that no sweep has run, because the sweep does not run on a laptop. A "Run now" button on the Jobs page answers "Starting desk jobs needs the Datatailr platform." When you finish, press Ctrl+C to stop the app. Then stop the two services with `kill %1 %2`.

## Deploy to Datatailr

### Prepare the cluster once

An administrator prepares the cluster once. Log in with `datatailr login`, then run this command from the repository root:

```bash
python merit_desk/admin/setup_demo.py --research 500 --trading-desk 300
```

The amounts are in USD. The script does four things:

- It creates the groups `research`, `trading_desk` and `platform`. Group names cannot contain spaces.
- It creates the budgets "Research" and "Trading desk". The "Research" budget has `prevent_overflow` on. Once the spending of the current month reaches the amount, the scheduler stops the running and pending jobs of that budget within about a minute. It also removes their scheduled runs.
- It sets the variables `markets` to `GB,DE,FR` and `sweep_size` to `small`, but only if they have no value yet.
- It prints a reminder about the secret, which is described below.

You must run `setup_demo.py` before the first deploy, because the deploy looks up the budgets "Trading desk" and "Research" by name. The daily curve, the services, the app, the run workflows of the app and the add-in use "Trading desk". The sweep and the training use "Research".

You can run the script again. It leaves a group or a budget that exists as it is. It also keeps a variable that already has a value, so a run does not undo `sweep_size=full` or a changed market list. The script writes the variables of the current environment only, because dev, pre and prod each have their own variables. You set the variables of pre and prod the same way, from that environment.

### Variables

Two variables control the workflows:

| Variable | Values | Default | Used by |
| --- | --- | --- | --- |
| `markets` | A comma list of market codes, for example `GB,DE` | `GB,DE,FR` | The daily curve |
| `sweep_size` | `small` (24 backtests) or `full` (342 backtests) | `small` | The strategy sweep |

A workflow reads a variable at the time it builds its graph of tasks. As a result, a change applies at the next deploy or the next instant run. It does not apply at the next scheduled run. To change a variable, use the SDK, the `datatailr` Python package:

```python
from datatailr import KV

KV().put("markets", "GB,DE")
```

### The secret

The chat message needs the secret `desk_webhook_url`. Its value is the URL of a chat webhook, a URL that accepts messages. An administrator creates the secret in the Secret Keys Manager, in each environment. The SDK cannot create secrets.

The secret is optional. Without it, the daily curve skips the chat message, writes a warning to its log and still succeeds.

Keep the logging of `urllib3` off the DEBUG level. DEBUG logging prints request paths, and the path of a webhook URL is part of the secret.

### The GPU node

The demand training needs a GPU node, a machine with a GPU. An administrator starts the node by hand, because the autoscaler does not start GPU nodes. The autoscaler adds and removes nodes as the load changes.

### Deploy the jobs

Log in with `datatailr login`. Then run one of these commands from the repository root. The script deploys to the dev environment, which is the SDK default. You get pre and prod by promoting jobs from dev.

```bash
python merit_desk/deploy.py workflows   # Daily Curve and Strategy Sweep
python merit_desk/deploy.py training    # Demand Training, which needs a GPU node
python merit_desk/deploy.py services    # Pricing API and Market Feed
python merit_desk/deploy.py app         # Merit Desk, the FastAPI desk app
python merit_desk/deploy.py excel       # Desk Tools
python merit_desk/deploy.py all         # Everything except demand training (the default)
```

The target `all` deploys everything except demand training. Deploy the training with `training`, after an administrator starts a GPU node. The training has no schedule, so Datatailr starts one run at deploy, and that run waits for the GPU node.

The app appears as "Merit Desk" in the "Trading" section of the launcher. Before the first daily curve, the Pricing API answers "No curve yet", and the Curves page shows that message. To make the first curve at once, run the notebook `01_instant_run.ipynb`, which the next section describes.

### The notebooks

The two notebooks in `merit_desk/notebooks/` use the platform. Run them on a Datatailr workstation, from a clone of this repository.

- `01_instant_run.ipynb` runs the daily curve as an instant run, which starts the workflow without a new build. Run the cells in order. The last cell reads the result of the task `curve_DE`. Run it after the widget shows that the run has finished, because the result is empty until then.
- `02_tune_demand_model.ipynb` spreads a scikit-learn search over 16 Datatailr workers with joblib.

Before you run a notebook:

- Build the workstation image from `merit_desk/requirements-dev.txt`. The "Pricing Research" workstation image is built from that file. The file lists `cloudpickle`. In Jupyter, an instant run sends its tasks with cloudpickle, and the SDK raises an error if the image does not list it.
- You need no path setup. The first cell of each notebook puts the repository root on `sys.path`, so the notebook finds the `merit_desk` package.

## Things to know

- The newest ECMWF run is not always out at 07:00 London. The Curves page shows the source of each curve, which includes the run time of its forecast, so you can see which run a curve uses.
- If the download fails, the daily curve uses the newest saved forecast, or the sample forecast. It writes a warning to its log.
- The Market Feed simulates the live prices. It moves each price a little every second, around the front-month price of the newest curve. Before the first curve exists, it uses the stack price at the normal temperature of the day. The demo uses no real market prices. The top bar of the app shows these prices and says that they are simulated.
- Demand and prices come from the stack model. The history for the backtests and the training is made up.
- In an Excel formula, write the month of `curve_price` as text, for example `"2027-01"`. A month typed into a cell can become a date.
- On the platform, each "Run as job" is a Datatailr job, and a run takes about 10 to 30 seconds, because Datatailr starts a container for it. All the runs of the app use the budget "Trading desk".
- At most three runs are in progress at a time, for the whole app. A fourth click gets this message: "Three runs are in progress. Wait for one to finish." The "Run now" buttons on the Jobs page do not count.
- A run that you start with "Run as job" fails as timed out when it shows no change for 15 minutes. The app follows a run that you start with "Run now" for up to six hours.
- The app keeps the workflow of a run for about 10 minutes after the run ends, so that you can still see it in Datatailr. Then it removes the workflow, whether the run succeeded or failed. The app waits until the platform also shows the run as ended. If the platform still shows the run as running 15 minutes after the app saw it end, the app stops it and removes it. When the app starts, it removes the leftover run workflows that are older than one hour. It also takes up again the runs that were not finished.
- The app checks each input before it starts a run, and answers with the reason when an input is out of range. The shift is from -10 to +10 °C, and its window of dates lies inside the 12 months of the curve. A run uses 100 to 5,000 weather paths. A plant costs 0 to 1,000 EUR/MWh and has 0 to 200 GW of capacity. The tenor is 1 to 6 months, and the threshold is 0.25 to 3.0 °C.
- The Scenario preview reads the newest saved forecast from blob storage, or the sample forecast. Neither preview calls the Pricing API, so both work when it is down. The Curves page needs the Pricing API.
- The Jobs page starts the Daily Curve and the Strategy Sweep under the identity of the app. So that identity needs the right to operate both workflows. If the platform refuses a start, the page shows the message of the platform. A run or a start that the platform refuses turns its card in the rail to failed, with the message of the platform. The access list of the app decides who can open the app.

In Excel, the add-in gives you three functions: `stack_price(market, temp_c)`, `curve_price(market, month)` and `live_price(market)`. The first one needs no service. For example, `=stack_price("DE", 4.5)` gives the German price at 4.5 °C, which is 170 EUR/MWh (euros per megawatt-hour).

## Data

The temperatures come from the ECMWF open data, from IFS HRES, the high-resolution forecast model of ECMWF. The desk uses the 2 m temperature only. The app shows this credit in its footer:

> Temperature forecasts: based on data and products of the European Centre for Medium-Range Weather Forecasts (ECMWF), licensed under CC BY 4.0 (https://creativecommons.org/licenses/by/4.0/). Modified: daily mean 2 m temperature at one grid point per city.

Use this copyright line, with the year of the forecast run:

© <year of the forecast run> European Centre for Medium-Range Weather Forecasts (ECMWF)

The sample forecast in this repository is the run of 2026-10-29 00z. For it, the line reads: © 2026 European Centre for Medium-Range Weather Forecasts (ECMWF).

ECMWF gives this disclaimer:

> "ECMWF does not accept any liability whatsoever for any error or omission in the data, their availability, or for any loss or damage arising from their use."

- ECMWF open data: https://www.ecmwf.int/en/forecasts/datasets/open-data
- ECMWF terms of use: https://apps.ecmwf.int/datasets/licences/general/
