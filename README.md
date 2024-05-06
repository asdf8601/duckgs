# DuckDB SQL CLI for Google Cloud Storage

`duckgs` is a command-line interface (CLI) tool that allows you to query
Parquet files in Google Cloud Storage using SQL. It is designed to simplify
the process of querying and analyzing data stored in Google Cloud Storage.

The tool leverages the power of DuckDB, an in-memory analytical database
written in C++. DuckDB provides a robust and efficient SQL interface to your
data, and `duckgs` brings this functionality to your command line, allowing you
to execute SQL queries directly on y our data stored in Google Cloud Storage.


## Installation

The recommended way is using [`pipx`][pipx]:

```bash
pipx install ssh://git@github.com/mmngreco/duckgs
duckgs --help
```

> [!warning]
>
> Currently, Python 3.12 cannot be used. Please use Python version 3.11 or
> lower.


### Alternative way

```bash
git clone ssh://git@github.com/mmngreco/duckgs
cd duckgs
python -m venv venv
source ./venv/bin/activate
pip install .
# choose you shell
echo "alias duckgs='$(which duckgs)'" >> ~/.bashrc
echo "alias duckgs='$(which duckgs)'" >> ~/.zshrc
# reload you shell
duckgs --help
```

## Motivation

The motivation behind `duckgs` is to provide a simple and efficient way to
interact with large datasets stored in Google Cloud Storage. As data grows, it
becomes increasingly difficult to analyze and derive insights from it.
Traditional methods of downloading the data and analyzing it locally become
impractical due to the sheer size of the data.

`duckgs` solves this problem by allowing you to query the data directly where
it resides - in Google Cloud Storage. This eliminates the need to download
large datasets, and allows you to quickly and efficiently analyze your data
using familiar SQL syntax.

## Features

- **SQL Interface**: `duckgs` provides a SQL interface to your data, allowing
  you to use familiar SQL syntax to query and analyze you r data.

- **In-Memory Analysis**: `duckgs` leverages DuckDB's in-memory analytical
  capabilities to provide fast and efficient analysis of you r data.

- **Command Line Interface**: `duckgs` is a CLI tool, which means it can be
  easily integrated into your data processing pipelines.

- **Query Caching**: `duckgs` caches the results of your queries, making
  subsequent queries faster.

- **Support for Placeholders**: `duckgs` supports the use of placeholders in
  your queries, allowing you to easily parameterize your queries.

For more detailed usage instructions and examples, you can run `duckgs
--examples` or `duckgs -x` in your command line.


[pipx]: https://pypa.github.io/pipx/installation/
