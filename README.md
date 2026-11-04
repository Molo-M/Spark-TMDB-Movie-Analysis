# TMDB Movie Data Analysis using Spark and PySpark

A data analysis pipeline built with **Apache Spark (PySpark)** that fetches data for 18 blockbuster movies from the [TMDB API](https://developer.themoviedb.org/docs), cleans and transforms it, and analyses which movies, franchises and directors performed best financially and with audiences.

This is a rebuild of an [earlier version of the project written with Pandas](https://github.com/Molo-M/TMDB-Movie-Data-Analysis). Both versions produce the same results; this one uses Spark so the same pipeline could scale to millions of movies.

### Project Links

| Deliverable | Link |
| --- | --- |
| **Project Instructions** | [View Instructions](Project_Instructions_Descriptions.md) |
| **Jupyter Notebook** | [View Notebook](notebooks/Movie-Analysis-Notebook.ipynb) |
| **Final Report** | [View Report](reports/Final_Report.pdf) |
| **Previous Project Version (Pandas, Numpy, Seaborn)** | [View Previous Project](https://github.com/Molo-M/TMDB-Movie-Data-Analysis) |



## Repository Structure

```text
Spark-TMDB-Movie-Analysis/
├── data/
│   ├── raw/
│   │   └── movies_raw.jsonl           # API response, one movie per line (JSON Lines)
│   └── processed/
│       └── movies_clean.parquet/      # clean data (Spark writes a folder of files)
├── notebooks/
│   └── Movie-Analysis-Notebook.ipynb  # step-by-step walkthrough of the analysis
├── reports/
│   ├── Final_Report.pdf               # summary of insights, methodology and conclusions
│   └── figures/                       # charts saved by main.py
├── src/
│   ├── __init__.py
│   ├── config.py                      # movie IDs, API and Spark settings, paths, column lists
│   ├── spark_session.py               # creates the Spark session used everywhere
│   ├── schemas.py                     # explicit schema of the raw API data
│   ├── fetch.py                       # Step 1: API extraction (with timeout and retries)
│   ├── clean.py                       # Step 2: cleaning and preprocessing
│   ├── analysis.py                    # Step 3: KPIs, UDF, searches, franchise/director stats
│   └── visualize.py                   # EDA and Step 4 charts
├── check_setup.py                     # checks that Spark works on your machine
├── main.py                            # runs the full pipeline end to end
├── requirements.txt
├── Project_Instructions_Descriptions.md
├── .env.example                       # template for the API token
├── .gitignore
└── README.md
```

---

## Requirements

- **Python 3.11.** On Windows, PySpark's Python workers crash with Python 3.12 and newer, so use 3.11.
- **Java 17** (Spark runs on Java). Spark 4 also supports Java 21 and 25, but not older versions.
- **A TMDB API Read Access Token**, free from your [TMDB account settings](https://www.themoviedb.org/settings/api).
- **Windows only:** Spark needs `winutils.exe` and `hadoop.dll` to write files. Put both in a folder such as `C:\hadoop\bin`, set the `HADOOP_HOME` environment variable to `C:\hadoop`, and add `%HADOOP_HOME%\bin` to `Path`.

---

## How to Run

1. Clone the repository and move into the project folder.
2. Create a virtual environment with Python 3.11 and install the dependencies:
   ```bash
   python -m venv venv            # on Windows: py -3.11 -m venv venv
   source venv/bin/activate       # on Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```
3. Copy `.env.example`, rename the copy to `.env`, and add your TMDB API Read Access Token.
4. Check that Spark works on your machine:
   ```bash
   python check_setup.py
   ```
   It should end with `All checks passed!`
5. Run the full pipeline:
   ```bash
   python main.py              # uses the saved raw data in data/raw/
   python main.py --refresh    # fetches fresh data from the API instead
   ```
   This prints all the results and saves the charts to `reports/figures/`.
6. To explore the analysis step by step, open `notebooks/Movie-Analysis-Notebook.ipynb` and select the virtual environment as the kernel.

The raw data is committed, so steps 5 and 6 reproduce the exact results in the report without calling the API. Fetching fresh data with `--refresh` gives slightly different popularity, vote and rating figures, because TMDB updates them constantly.

---

## Methodology

**1. Data extraction (`fetch.py`, `schemas.py`)**
Each movie is requested from the TMDB `/movie/{id}` endpoint together with its credits. Requests have a 10-second timeout, and temporary failures (network errors, rate limits, server errors) are retried up to 3 times. Invalid IDs, such as `0`, are reported and skipped. The response is saved as JSON Lines and read into a Spark DataFrame with an explicit schema, so column types are defined rather than guessed, and values of the wrong type are caught.

**2. Cleaning and preprocessing (`clean.py`)**
Irrelevant columns are dropped. Nested JSON columns (collection, genres, languages, countries, companies, credits) are flattened into readable `|`-separated names with Spark's built-in functions, and cast size, crew size and director(s) are extracted from the credits. Inspecting the columns with value counts revealed blank language names in the TMDB data, so blank names are skipped and spoken languages use their English names. Data types are converted with `try_cast` and `try_to_date`, which turn invalid values into null instead of stopping the job. Unrealistic values (zero budget/revenue/runtime, ratings with no votes, placeholder text) are replaced with null, and budget and revenue are converted to million USD. Duplicates, rows missing an id or title, rows with fewer than 10 non-null values, and unreleased movies are removed.

**3. Exploratory data analysis (notebook and `visualize.py`)**
Summary statistics, distributions of budget, revenue, rating and runtime, genre counts, release years, and correlations between the numeric columns (computed with Spark MLlib).

**4. KPIs and analysis (`analysis.py`)**
Profit (revenue − budget) and ROI (revenue ÷ budget) are calculated. Movies are ranked on revenue, budget, profit, ROI (budget ≥ $10M only), votes, rating (≥ 10 votes only) and popularity by a single ranking function that uses a Spark window function, so tied movies share a rank. A User-Defined Function (UDF) formats the money rankings as readable labels (e.g. `$2.94B`). The analysis also includes two cast/director search queries, a franchise vs. standalone comparison, and summary tables for franchises and directors.

**5. Visualization (`visualize.py`)**
Spark does the aggregation for each chart, and only the small result is converted to Pandas for Matplotlib to draw: revenue vs. budget, ROI by genre, popularity vs. rating, yearly box office trends, and franchise vs. standalone performance.

---

## Key Findings

- **All 18 movies are major blockbusters.** Revenues range from \$1.24B to \$2.94B, and even the least profitable movie (*Star Wars: The Last Jedi*) made over \$1B in profit. Results therefore describe top-grossing movies, not movies in general.
- **Avengers: Endgame earned the most revenue (\$2.94B), but Avatar was the better investment.** *Avatar* has the highest profit (\$2.69B) and the highest ROI, earning 12.3 times its budget.
- **The Avengers Collection is the most successful franchise.** Its 4 movies earned \$7.92B in total, with a mean rating of 7.96. The four most popular movies are all Avengers films.
- **Franchise vs. standalone results are not conclusive.** Franchise movies had bigger budgets (\$218M vs. \$180M on average) and slightly higher popularity. The 2 standalone movies (*Titanic* and *Beauty and the Beast*) had slightly higher mean revenue and median ROI, but 2 movies is too small a sample to compare fairly.
- **James Cameron is the top director by revenue** (\$5.19B from *Avatar* and *Titanic*). Anthony and Joe Russo have the highest mean rating (8.25) for *Avengers: Infinity War* and *Endgame*.
- **Popularity and rating are closely linked (0.83), while budget is only moderately linked to revenue (0.58).**
- **Neither search query returned results.** No movie in the dataset stars Bruce Willis, or stars Uma Thurman under Quentin Tarantino's direction.

The data was collected from TMDB in November 2026. See the [notebook](notebooks/Movie-Analysis-Notebook.ipynb) for the full analysis and the [report](reports/Final_Report.pdf) for a summary of insights and conclusions.

---

## Pandas vs. Spark

The [Pandas version](PANDAS_REPO_URL) and this Spark version give the same results. The main differences in how the work is done:

- **Built-in functions instead of Python loops.** Nested JSON is flattened with Spark column expressions that run in parallel, instead of Python functions applied row by row.
- **Lazy evaluation and caching.** Spark builds a plan of all the steps and only runs it when a result is needed; the clean data is cached because every later step reuses it.
- **Tuned for a small dataset.** Spark's shuffle partitions are reduced from 200 to 4, which suits 18 movies.
- **No row index.** Spark DataFrames have no index, so there is nothing to reset.
- **Ties share a rank.** Rankings use a window function, so tied movies are not cut off arbitrarily.
- **Spark aggregates, Matplotlib draws.** Only small aggregated results are collected for plotting, which is how charts are made from data too large for one machine.

---

## Tools

Python, Apache Spark (PySpark, Spark SQL, MLlib), Pandas, NumPy, Matplotlib, PyArrow, Requests, python-dotenv, Jupyter.