# Pandas Playground

A Python project for experimenting with pandas and DuckDB data analysis.

## Features

- Pandas 3.0+ for data manipulation
- DuckDB 1.4+ for fast SQL analytics
- Modern Python 3.12+ environment managed with uv

## Setup

1. Clone the repository:
```bash
git clone https://github.com/smjagadish/pandas_playground.git
cd pandas_playground
```

2. Install uv if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

3. Sync dependencies:
```bash
uv sync
```

4. Activate the virtual environment:
```bash
source .venv/bin/activate
```

## Usage

Run the main script:
```bash
python main.py
```

## Dependencies

- pandas >= 2.0.0
- duckdb >= 0.9.0

## License

MIT
