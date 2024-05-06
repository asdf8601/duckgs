r"""

          $$\                     $$\
          $$ |                    $$ |
     $$$$$$$ |$$\   $$\  $$$$$$$\ $$ |  $$\  $$$$$$\   $$$$$$$\
    $$  __$$ |$$ |  $$ |$$  _____|$$ | $$  |$$  __$$\ $$  _____|
    $$ /  $$ |$$ |  $$ |$$ /      $$$$$$  / $$ /  $$ |\$$$$$$\
    $$ |  $$ |$$ |  $$ |$$ |      $$  _$$<  $$ |  $$ | \____$$\
    \$$$$$$$ |\$$$$$$  |\$$$$$$$\ $$ | \$$\ \$$$$$$$ |$$$$$$$  |
     \_______| \______/  \_______|\__|  \__| \____$$ |\_______/
                                            $$\   $$ |
                                            \$$$$$$  |
                                             \______/

            DuckDB SQL CLI for Google Cloud Storage


duckgs is a CLI tool that allows you to query Parquet files in Google Cloud
Storage using SQL.

Examples
--------
$ duckgs --examples
or
$ duckgs -x
"""
import hashlib
import os
import re
import time
from pathlib import Path
from textwrap import dedent
from typing import Any

import duckdb
import pandas as pd  # noqa
import sqlparse
import typer
from fsspec import filesystem
from rich import print, print_json  # noqa
from rich.console import Console
from rich.status import Status
from rich.syntax import Syntax
from typer import Option

__all__ = ["print", "print_json", "app"]

TEMP = "/tmp/duckgs"
VERBOSE = False

# Inicializar Typer CLI y Rich
app = typer.Typer(rich_markup_mode="rich")
console = Console()


def print_examples():
    out = r"""
        _______  __ ___    __  _______  __    ___________
       / ____/ |/ //   |  /  |/  / __ \/ /   / ____/ ___/
      / __/  |   // /| | / /|_/ / /_/ / /   / __/  \__ \
     / /___ /   |/ ___ |/ /  / / ____/ /___/ /___ ___/ /
    /_____//_/|_/_/  |_/_/  /_/_/   /_____/_____//____/

    [yellow]Quick start:[/]

    $ duckgs --query "SELECT 42"

    [yellow]Silent mode:[/]

    $ duckgs --query "SELECT 42" --silent

    [yellow]All queries are cached:[/]

    $ duckgs --query "FROM read_parquet('gs://bucket/**/*.parquet') LIMIT 1"
    $ duckgs --query "FROM read_parquet('gs://bucket/**/*.parquet') LIMIT 1"

    [yellow]Simplify queries using placeholders:[/]

    $ duckgs --bucket "gs://bucket/**/*.parquet" /
             --query "select * from read_parquet('{bucket}')" /

    [yellow]This is equivalent to:[/]

    $ duckgs --query "FROM read_parquet('{bucket}') LIMIT 1" /
             --kwargs "{'bucket': 'gs://bucket/**/*.parquet'}"

    [yellow]You can also more complex placeholders in the query:[/]

    $ duckgs --bucket "gs://bucket/**/*.parquet" /
             --query "select {cols} from read_parquet('{bucket}')" /
             --kwargs "{'cols': 'bidfloor, hour'}" /

    [yellow]Or even use env-vars:[/]

    $ DUCKGS_BUCKET=gs://bucket/**/*.parquet duckgs --query "select 42 from read_parquet('{bucket}')"

    [yellow]From file (equivalent to --query but reading a file):[/]

    $ echo "SELECT * FROM read_parquet('gs://bucket/*.parquet') limit 1" > /tmp/query.sql
    $ duckgs --query-file /tmp/query.sql

    [yellow]Modify the output:[/]

    $ duckgs --query-file /tmp/query.sql --eval-df "df.T"
    $ duckgs --query-file /tmp/query.sql --eval-df "pd.Series(df.columns)"
    $ duckgs --query-file /tmp/query.sql --eval-df "df.to_markdown()"
    $ duckgs --query-file /tmp/query.sql --eval-df "df.to_csv()"

    [yellow]Save the output to a file in your favorite format:[/]

    $ duckgs --query-file /tmp/query.sql --eval-df "df.to_csv()" -s > /tmp/out.csv
    $ cat /tmp/out.csv
    """
    out = dedent(out)
    console.print(out)


def find_placeholders(query: str):
    placeholders = re.findall(r'\{(.+?)\}', query)
    return placeholders


def ask_user_for_values(placeholders: list[str]):
    values = {}
    for placeholder in placeholders:
        value = console.input(
            f"Please provide a value for [bold green]{placeholder}[/]: ",
            markup=True,
        )
        values[placeholder] = value
    return values


def fill_placeholders(query: str) -> str:
    placeholders = find_placeholders(query)
    kwargs_dict = ask_user_for_values(placeholders)
    query = format(query, **kwargs_dict)
    return query


def format(string: str, **kwargs):
    try:
        out = string.format(**kwargs)
    except KeyError:
        out = string
    return out


def mkdir(path: str):
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)


def generate_cache_filename(query: str) -> str:
    hash_digest = hashlib.md5(query.encode('utf-8')).hexdigest()
    return f"{TEMP}/cache_{hash_digest}.pkl"


def cachify(func):
    from functools import wraps

    mkdir(TEMP)

    @wraps(func)
    def wrapper(*args, **kwargs):
        cache_filename = generate_cache_filename(*args, **kwargs)
        if os.path.exists(cache_filename):
            if VERBOSE:
                console.print(
                    f"Loading from cache: {cache_filename}",
                    style="bold yellow",
                )
            return pd.read_pickle(cache_filename)
        df = func(*args, **kwargs)
        df.to_pickle(cache_filename)
        if VERBOSE:
            console.print(f"Query cached: {cache_filename}")
        return df

    return wrapper


def read_query_from_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        return file.read().strip()


@cachify
def duckdb_query(query: str) -> pd.DataFrame:
    global query_time
    import warnings

    with warnings.catch_warnings():
        # skip warnings from DuckDB
        warnings.filterwarnings("ignore", category=Warning)
        duckdb.register_filesystem(filesystem("gs"))

    with Status("Executing query...\n", console=console, spinner="dots2"):
        tick = time.time()
        df = duckdb.query(query).df()
        tock = time.time()
        query_time = tock - tick
        if VERBOSE:
            console.print(f"Query took: {query_time:.2f} seconds")
    return df


def ensure_bucket(bucket: str) -> str:
    if not bucket:
        return ""
    return bucket if bucket.startswith("gs://") else f"gs://{bucket}"


def build_query(
    query: str,
    query_file: str,
    kwargs: dict,
) -> str:
    if query:
        out = format(query, **kwargs)
    elif query_file:
        with open(query_file, 'r') as file:
            query = file.read().strip()
        out = format(query, **kwargs)
    else:
        console.print("Please provide a query or a query file.")
        raise typer.Exit(1)
    return out


def print_query(query: str) -> None:
    if VERBOSE:
        _query = sqlparse.format(
            query, reindent_aligned=True, keyword_case='upper'
        )
        _query = Syntax(
            _query,
            "sql",
            theme="github-dark",
            line_numbers=True,
            indent_guides=True,
        )
        # input
        console.print("[green]In\\[query]:\n[/]", no_wrap=True)
        console.print(
            _query,
            crop=False,
            no_wrap=True,
            overflow="ellipsis",
            soft_wrap=False,
            style="bold blue",
        )


def print_result(out: Any, key="") -> None:
    if VERBOSE:
        if key:
            key = f"\\[{key}]"
        console.print(f"\n[green]Out{key}:[/]\n", no_wrap=True)
    console.print(
        out,
        crop=False,
        no_wrap=True,
        overflow="fold",
        soft_wrap=False,
        style="bold green",
    )


def run_eval_df(eval_df, query, df):
    # manipulate result
    if isinstance(eval_df, list):
        for code in eval_df:
            return run_eval_df(code, query, df)

    df = eval(eval_df)
    if VERBOSE:
        console.print("\n[green]In\\[eval-df][/]:\n")
        console.print(eval_df)
        console.print("\n[green]Out\\[eval-df]:[/]\n")
        console.print(df)
    return df


def run_script(script, globals, locals):
    # execute script
    if VERBOSE:
        _script = Syntax(
            script,
            "python",
            theme="github-dark",
            line_numbers=True,
            indent_guides=True,
        )
        console.print("\n[green]In\\[script]:[/]\n")
        console.print(_script)
        console.print("\n[green]Out\\[script]:[/]\n")

    import code

    compiled = code.compile_command(script)
    if compiled is not None:
        exec(compiled, globals, locals)


def run_script_file(script_file, globals, locals):
    # execute script
    with open(script_file, 'r') as file:
        script = file.read().strip()

    if VERBOSE:
        _script = Syntax(
            script,
            "python",
            theme="github-dark",
            line_numbers=True,
            indent_guides=True,
        )
        console.print("\n[green]In\\[script]:[/]n")
        console.print(_script)
        console.print("\n[green]Out\\[script]:[/]n")
    exec(script, globals, locals)


help = """
[bold yellow]duckgs[/] is a CLI tool that allows you to query Parquet files in
Google Cloud Storage using SQL.
"""
epilog = "Made with :heart:  by MaxGreco."


@app.command(help=help, epilog=epilog)
def cli(
    query: str = Option(
        None,
        "--query",
        "-q",
        help="SQL query to execute.",
        rich_help_panel="Query",
    ),
    bucket: str = Option(
        None,
        "--bucket",
        "-b",
        envvar="DUCKGS_BUCKET",
        help="bucket in GCS. You can use `{bucket}` placeholder.",
        rich_help_panel="Query",
    ),
    query_file: str = Option(
        None,
        "--query-file",
        "-f",
        help="Read query from file.",
        rich_help_panel="Query",
    ),
    kwargs: str = Option(
        "{}",
        "--kwargs",
        "-k",
        envvar="DUCKGS_KWARGS",
        help="Extra args for formatting the query, e.g., --kwargs \"{'year': 2021}\"",
        rich_help_panel="Query",
    ),
    silent: bool = Option(
        False,
        "--silent",
        "-s",
        help="It will only print the result.",
        rich_help_panel="Output",
    ),
    examples: bool = Option(
        False,
        "--examples",
        "-x",
        help="Show examples. e.g `duckgs -x | less`",
        rich_help_panel="Help",
    ),
    eval_df: list[str] = Option(
        [],
        "--eval-df",
        "-e",
        help="Modify the DataFrame (`df`). e.g. --eval-df \"df.T\". It's evaluated just after the query is executed.",
        rich_help_panel="Extra Modifications",
    ),
    script: str = Option(
        None,
        "--script",
        "-S",
        help=(
            "Execute a script. This is executed after --eval-df. Data is "
            "available in `df` variable, overwrite the variable to persist "
            "changes. You can use `print` or `print_json` functions to show "
            "intermediate results. Use `:` as a separator. e.g. "
            "--script \"print(df); df.T\""
        ),
        rich_help_panel="Extra Modifications",
    ),
    script_file: str = Option(
        None,
        "--script-file",
        "-F",
        help=(
            "Execute a script from file. Use it instead of --script. Data is "
            "available in `df` variable, overwrite the variable to persist "
            "changes. You can use print or pron_json functions to show "
            "intermediate results."
        ),
        rich_help_panel="Extra Modifications",
    ),
    ipython: bool = Option(
        False,
        "--ipython",
        "-i",
        help="Run an IPython shell after executing the query.",
        rich_help_panel="Extra Modifications",
    ),
):
    global VERBOSE
    VERBOSE = not silent

    if examples:
        print_examples()
        raise typer.Exit()

    kwargs_dict = eval(kwargs)
    bucket = ensure_bucket(bucket)
    fmt_kws = {"bucket": bucket}
    fmt_kws.update(kwargs_dict)

    query = build_query(query, query_file, fmt_kws)
    query = fill_placeholders(query)
    query = dedent(query)
    print_query(query)

    # result
    df = duckdb_query(query)
    if VERBOSE:
        print_result(df, key="query")

    if eval_df:
        df = run_eval_df(eval_df, query, df)

    if script:
        run_script(script, globals(), locals())

    elif script_file:
        run_script_file(script_file, globals(), locals())

    else:
        print_result(df, key="df")

    if ipython:
        # make it optional depencency
        from IPython import embed

        embed()


if __name__ == "__main__":
    app()

# vim: tw=0
