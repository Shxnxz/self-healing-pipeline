import sys
import os
import glob
import json
import webbrowser

# Fix Windows encoding for unicode box drawing & symbols in terminal
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

# pyrefly: ignore [missing-import]
import duckdb
import pandas as pd

# Check Rich library for beautiful terminal formatting
try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.syntax import Syntax
    from rich import box
    RICH_AVAILABLE = True
    console = Console()
except ImportError:
    RICH_AVAILABLE = False
    console = None

# Determine project root path dynamically
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

def resolve_layer_path(relative_path: str) -> str:
    """Resolves path whether script is executed from project root or Read Data/ folder."""
    if os.path.exists(os.path.dirname(relative_path)):
        return relative_path
    from_root = os.path.join(PROJECT_ROOT, relative_path.lstrip("./"))
    return from_root

# Mapping of folder names for your pipeline layers
LAYERS = {
    "bronze": resolve_layer_path("./delta/bronze/part-*.parquet"),
    "silver1": resolve_layer_path("./delta/silver1/part-*.parquet"),
    "silver2": resolve_layer_path("./delta/silver2/part-*.parquet"),
    "gold": resolve_layer_path("./delta/gold/part-*.parquet"),
    "audit": resolve_layer_path("./delta/silver2_fuzzy_audit/part-*.parquet"),
    "quarantine": resolve_layer_path("./delta/silver2_quarantine/part-*.parquet"),
    "ref": resolve_layer_path("./delta/ref_canonical_mappings/part-*.parquet"),
    # Legacy aliases
    "silver": resolve_layer_path("./delta/silver1/part-*.parquet"),
    "silver_cars": resolve_layer_path("./delta/silver_cars/part-*.parquet"),
    "gold_car_overview": resolve_layer_path("./delta/gold_car_overview/part-*.parquet"),
}

def format_val(val) -> str:
    """Format cell value with clean styles for NULLs, numbers, and booleans."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or str(val) == "nan":
        return "[dim italic]null[/dim italic]" if RICH_AVAILABLE else "null"
    s = str(val).strip()
    if s == "":
        return "[dim]—[/dim]" if RICH_AVAILABLE else "—"
    if s.lower() in ("true", "false") and RICH_AVAILABLE:
        return f"[bold yellow]{s}[/bold yellow]"
    try:
        float(s)
        return f"[bright_cyan]{s}[/bright_cyan]" if RICH_AVAILABLE else s
    except ValueError:
        pass
    return s

def print_banner(target_name: str, total_count: int, total_cols: int, preview_count: int, mode: str):
    """Prints a styled header with dataset metadata."""
    if RICH_AVAILABLE:
        summary_text = (
            f"[bold cyan]Layer / Target:[/bold cyan] [white]{target_name}[/white]   │   "
            f"[bold cyan]Total Rows:[/bold cyan] [green]{total_count:,}[/green]   │   "
            f"[bold cyan]Total Columns:[/bold cyan] [yellow]{total_cols}[/yellow]   │   "
            f"[bold cyan]Display Mode:[/bold cyan] [magenta]{mode.upper()}[/magenta]"
        )
        console.print(Panel(summary_text, title="[bold white on blue] 📊 Parquet Data Inspector [/bold white on blue]", border_style="blue"))
    else:
        print(f"\n=== Parquet Inspector: {target_name} ({total_count:,} rows, {total_cols} columns) [Mode: {mode}] ===")

def render_schema_view(df: pd.DataFrame):
    """Displays all columns, data types, null counts, and a clean sample value in a compact table."""
    if RICH_AVAILABLE:
        table = Table(
            title=f"Dataset Schema & Column Catalog ({len(df.columns)} Columns)",
            header_style="bold magenta",
            box=box.ROUNDED,
            border_style="blue",
            show_lines=False
        )
        table.add_column("#", style="dim", justify="right", width=3)
        table.add_column("Column Name", style="bold cyan", width=25)
        table.add_column("Type", style="green", width=10)
        table.add_column("Populated", justify="right", style="yellow", width=10)
        table.add_column("Sample Value", style="white")

        c_width = console.width if console else 80
        for idx, col in enumerate(df.columns, start=1):
            dtype = str(df[col].dtype)
            non_null = df[col].notna().sum()
            pct = (non_null / len(df)) * 100 if len(df) > 0 else 0
            first_valid = df[col].dropna()
            sample = str(first_valid.iloc[0]) if len(first_valid) > 0 else "[dim]null[/dim]"
            pop_str = f"{pct:.0f}%" if c_width < 100 else f"{non_null}/{len(df)} ({pct:.0f}%)"
            table.add_row(str(idx), col, dtype, pop_str, sample)

        console.print(table)
    else:
        print(f"{'#':<4} {'Column Name':<30} {'Data Type':<15} {'Sample Value'}")
        print("-" * 75)
        for idx, col in enumerate(df.columns, start=1):
            first_valid = df[col].dropna()
            sample = str(first_valid.iloc[0]) if len(first_valid) > 0 else "null"
            print(f"{idx:<4} {col:<30} {str(df[col].dtype):<15} {sample[:30]}")

def render_card_view(df: pd.DataFrame, force_vertical: bool = False, force_grid: bool = False):
    """Renders rows as individual cards with all columns displayed cleanly without messy horizontal overflow."""
    cols = list(df.columns)
    c_width = console.width if RICH_AVAILABLE else 80
    use_split = (not force_vertical) and (force_grid or (c_width >= 100 and len(cols) > 10))

    for row_idx, (_, row) in enumerate(df.iterrows(), start=1):
        if RICH_AVAILABLE:
            if use_split:
                mid = (len(cols) + 1) // 2
                col_l = [(i + 1, cols[i]) for i in range(mid)]
                col_r = [(i + 1, cols[i]) for i in range(mid, len(cols))]

                t = Table(box=None, padding=(0, 1), show_header=True, header_style="bold magenta")
                t.add_column("#", style="dim", justify="right", width=3)
                t.add_column("Field", style="bold cyan", width=26, no_wrap=True)
                t.add_column("Value", style="white", width=22)
                t.add_column("│", style="dim", width=1)
                t.add_column("#", style="dim", justify="right", width=3)
                t.add_column("Field", style="bold cyan", width=26, no_wrap=True)
                t.add_column("Value", style="white", width=22)

                for i in range(mid):
                    idx_1, name_1 = col_l[i]
                    val_1 = format_val(row[name_1])
                    if len(val_1) > 22 and "[" not in val_1:
                        val_1 = val_1[:19] + "..."

                    if i < len(col_r):
                        idx_2, name_2 = col_r[i]
                        val_2 = format_val(row[name_2])
                        if len(val_2) > 22 and "[" not in val_2:
                            val_2 = val_2[:19] + "..."
                        t.add_row(str(idx_1), name_1, val_1, "│", str(idx_2), name_2, val_2)
                    else:
                        t.add_row(str(idx_1), name_1, val_1, "│", "", "", "")
            else:
                t = Table(box=box.SIMPLE_HEAD, padding=(0, 1), show_header=True, header_style="bold magenta")
                t.add_column("#", style="dim", justify="right", width=4)
                t.add_column("Field Name", style="bold cyan", width=30, no_wrap=True)
                t.add_column("Value", style="white")
                for idx, col_name in enumerate(cols, start=1):
                    t.add_row(str(idx), col_name, format_val(row[col_name]))

            console.print(Panel(
                t,
                title=f"[bold green]Record #{row_idx}[/bold green] [dim](Row {row_idx} of {len(df)} previewed - {len(cols)} columns)[/dim]",
                border_style="bright_blue",
                expand=False
            ))
        else:
            print(f"\n--- Record #{row_idx} ({len(cols)} columns) ---")
            for idx, col_name in enumerate(cols, start=1):
                print(f"  [{idx:2d}] {col_name:<30} : {row[col_name]}")

def render_table_view(df: pd.DataFrame):
    """Renders rows as a horizontal table."""
    if RICH_AVAILABLE:
        table = Table(title="Tabular View", box=box.ROUNDED, header_style="bold cyan")
        table.add_column("#", style="dim", width=4)
        for col in df.columns:
            table.add_column(col, overflow="ellipsis", max_width=18)
        for idx, (_, row) in enumerate(df.iterrows(), start=1):
            row_vals = [str(idx)] + [format_val(row[c]) for c in df.columns]
            table.add_row(*row_vals)
        console.print(table)
    else:
        print(df.to_string())

def render_json_view(df: pd.DataFrame):
    """Outputs pretty-printed JSON records with syntax highlighting."""
    records = df.to_dict(orient="records")
    json_str = json.dumps(records, indent=2, default=str)
    if RICH_AVAILABLE:
        console.print(Syntax(json_str, "json", theme="monokai", line_numbers=True))
    else:
        print(json_str)

def generate_html_viewer(df: pd.DataFrame, target_name: str, total_count: int) -> str:
    """Generates an interactive standalone HTML table with DataTables and automatically opens it."""
    out_dir = os.path.dirname(os.path.abspath(__file__))
    out_file = os.path.join(out_dir, "read_parquet_preview.html")

    columns = list(df.columns)
    records = df.to_dict(orient="records")

    def esc(s):
        return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Parquet Inspector: {target_name}</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css">
    <link rel="stylesheet" href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css">
    <style>
        body {{
            background-color: #0b0f19;
            color: #e2e8f0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            padding: 24px;
        }}
        .card-custom {{
            background-color: #111827;
            border: 1px solid #1f2937;
            border-radius: 12px;
            padding: 24px;
            box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5);
        }}
        .table-responsive {{
            margin-top: 16px;
        }}
        table.dataTable {{
            color: #cbd5e1 !important;
            font-size: 0.85rem;
            white-space: nowrap;
        }}
        table.dataTable thead th {{
            background-color: #1f2937 !important;
            color: #38bdf8 !important;
            font-weight: 600;
            border-bottom: 2px solid #0284c7 !important;
        }}
        table.dataTable tbody tr {{
            background-color: #111827 !important;
        }}
        table.dataTable tbody tr:hover {{
            background-color: #1e293b !important;
        }}
        table.dataTable tbody td {{
            border-color: #1f2937 !important;
        }}
        .null-val {{
            color: #64748b;
            font-style: italic;
        }}
        .dataTables_info, .dataTables_paginate, .dataTables_length, .dataTables_filter {{
            color: #94a3b8 !important;
        }}
        .dataTables_filter input, .dataTables_length select {{
            background-color: #0b0f19 !important;
            border: 1px solid #374151 !important;
            color: #f3f4f6 !important;
            border-radius: 6px;
            padding: 4px 10px;
        }}
        .page-link {{
            background-color: #1f2937;
            border-color: #374151;
            color: #94a3b8;
        }}
        .page-item.active .page-link {{
            background-color: #0284c7;
            border-color: #0284c7;
            color: white;
        }}
    </style>
</head>
<body>
    <div class="container-fluid">
        <div class="card-custom">
            <div class="d-flex justify-content-between align-items-center mb-3">
                <div>
                    <h2 class="h4 mb-0 text-info">Parquet Dataset: {target_name}</h2>
                    <small class="text-secondary">Viewing all {len(columns)} columns with full interactive search & sorting</small>
                </div>
                <div class="d-flex gap-2">
                    <span class="badge bg-primary fs-6">Total Rows: {total_count:,}</span>
                    <span class="badge bg-success fs-6">Loaded: {len(df):,} rows</span>
                    <span class="badge bg-secondary fs-6">Columns: {len(columns)}</span>
                </div>
            </div>
            <div class="table-responsive">
                <table id="parquetTable" class="table table-bordered table-hover w-100">
                    <thead>
                        <tr>
                            <th>#</th>
                            {''.join(f'<th>{esc(c)}</th>' for c in columns)}
                        </tr>
                    </thead>
                    <tbody>
"""
    for idx, row in enumerate(records, start=1):
        tds = [f"<td>{idx}</td>"]
        for c in columns:
            v = row[c]
            if v is None or (isinstance(v, float) and pd.isna(v)) or str(v) == "nan":
                tds.append('<td class="null-val">null</td>')
            else:
                tds.append(f"<td>{esc(str(v))}</td>")
        html_content += f"<tr>{''.join(tds)}</tr>\n"

    html_content += """
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
    <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
    <script>
        $(document).ready(function() {
            $('#parquetTable').DataTable({
                scrollX: true,
                pageLength: 25,
                lengthMenu: [[10, 25, 50, 100, -1], [10, 25, 50, 100, "All"]],
                order: []
            });
        });
    </script>
</body>
</html>
"""
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(html_content)

    try:
        webbrowser.open("file://" + os.path.abspath(out_file))
    except Exception:
        pass
    return out_file

def print_help_tips(target_name: str):
    """Prints actionable usage commands and tips."""
    if RICH_AVAILABLE:
        tips = (
            f"[dim]💡 [bold white]Display Tips & Commands:[/bold white]\n"
            f"  • View more rows:       [cyan]python read_parquet.py {target_name} 5[/cyan]\n"
            f"  • Column catalog:       [cyan]python read_parquet.py {target_name} --schema[/cyan]  (or [cyan]-s[/cyan])\n"
            f"  • Single-column card:   [cyan]python read_parquet.py {target_name} --vertical[/cyan]  (or [cyan]-v[/cyan])\n"
            f"  • Filter columns:       [cyan]python read_parquet.py {target_name} --col injury,crash,speed[/cyan]\n"
            f"  • Interactive Browser:  [cyan]python read_parquet.py {target_name} --html[/cyan]  (or [cyan]-w[/cyan])\n"
            f"  • Tabular / JSON:       [cyan]python read_parquet.py {target_name} --table[/cyan] / [cyan]--json[/cyan][/dim]"
        )
        console.print(tips)
    else:
        print(f"\nTips: Run with '--schema' to see column types, or '--html' for interactive browser view.")

def main():
    raw_args = sys.argv[1:]

    # Defaults
    target_arg = "bronze"
    limit = 3  # default preview 3 rows for cards
    mode = "card"
    col_filter = None
    force_vertical = False
    force_grid = False

    # Check flags
    if "--schema" in raw_args or "-s" in raw_args:
        mode = "schema"
    elif "--html" in raw_args or "--web" in raw_args or "-w" in raw_args:
        mode = "html"
    elif "--table" in raw_args or "-t" in raw_args:
        mode = "table"
        limit = 10
    elif "--json" in raw_args or "-j" in raw_args:
        mode = "json"
    elif "--vertical" in raw_args or "-v" in raw_args:
        force_vertical = True
    elif "--grid" in raw_args or "-g" in raw_args:
        force_grid = True

    show_all = "--all" in raw_args or "-a" in raw_args

    # Check for column filter flag
    for idx, a in enumerate(raw_args):
        if a in ("--col", "--columns", "-c", "--filter", "-f") and idx + 1 < len(raw_args):
            col_filter = raw_args[idx + 1]

    # Extract positional layer/file and limit
    non_flags = [a for a in raw_args if not a.startswith("-")]
    # If the user passed an argument right after --col/--columns/-c/-f, don't treat it as layer name
    cleaned_non_flags = []
    skip_next = False
    for a in raw_args:
        if a in ("--col", "--columns", "-c", "--filter", "-f"):
            skip_next = True
        elif skip_next:
            skip_next = False
        elif not a.startswith("-"):
            cleaned_non_flags.append(a)

    if cleaned_non_flags:
        if cleaned_non_flags[0].isdigit():
            limit = int(cleaned_non_flags[0])
        else:
            target_arg = cleaned_non_flags[0]
            if len(cleaned_non_flags) > 1 and cleaned_non_flags[1].isdigit():
                limit = int(cleaned_non_flags[1])

    if show_all:
        limit = None

    # Resolve parquet path
    if os.path.exists(target_arg) or target_arg.endswith(".parquet"):
        parquet_pattern = target_arg
        target_name = target_arg
    elif target_arg in LAYERS:
        parquet_pattern = LAYERS[target_arg]
        target_name = target_arg
    else:
        print(f"Unknown layer '{target_arg}'. Available layers: {list(LAYERS.keys())}")
        sys.exit(1)

    try:
        # Check files exist
        matched = glob.glob(parquet_pattern)
        if not matched:
            print(f"\n[!] No Parquet files found at: {parquet_pattern}")
            print("Note: If the pipeline hasn't run yet, start it with `docker compose up` first to generate data.")
            return None

        # Row count
        count = duckdb.sql(f"SELECT COUNT(*) FROM '{parquet_pattern}'").fetchone()[0]

        # Query data
        query = f"SELECT * FROM '{parquet_pattern}'"
        if mode == "schema":
            df = duckdb.sql(f"{query} LIMIT 500").df()
        elif limit is not None:
            df = duckdb.sql(f"{query} LIMIT {limit}").df()
        else:
            df = duckdb.sql(query).df()

        # Apply column filtering if requested (e.g. --col injury,crash)
        if col_filter:
            keywords = [k.strip().lower() for k in col_filter.split(",")]
            # Allow basic stem matching: e.g. 'injury' also matches 'injuries'
            expanded_kw = set(keywords)
            for k in keywords:
                if k.endswith("y"):
                    expanded_kw.add(k[:-1] + "ies")
                    expanded_kw.add(k[:-1])
                elif k.endswith("ies"):
                    expanded_kw.add(k[:-3] + "y")
            matched_cols = [c for c in df.columns if any(k in c.lower() for k in expanded_kw)]
            if matched_cols:
                df = df[matched_cols]
            else:
                if RICH_AVAILABLE:
                    console.print(f"[yellow]Warning: No columns matched filter '{col_filter}'. Displaying all columns.[/yellow]")

        total_cols = len(df.columns)

        # Print banner
        print_banner(target_name, count, total_cols, len(df), mode)

        # Render chosen mode
        if mode == "schema":
            render_schema_view(df)
        elif mode == "html":
            if limit == 3 and not show_all:
                df_html = duckdb.sql(f"{query} LIMIT 100").df()
            else:
                df_html = df
            html_path = generate_html_viewer(df_html, target_name, count)
            if RICH_AVAILABLE:
                console.print(f"[bold green]✔ Interactive HTML viewer opened in your browser![/bold green] ([dim]{html_path}[/dim])")
            else:
                print(f"Interactive HTML viewer opened at: {html_path}")
        elif mode == "table":
            render_table_view(df)
        elif mode == "json":
            render_json_view(df)
        else:
            # Default: Card View keeping all columns
            render_card_view(df, force_vertical=force_vertical, force_grid=force_grid)

        # Print tips
        print_help_tips(target_name)

        return df

    except duckdb.IOException as e:
        print(f"\n[!] Parquet I/O error at {parquet_pattern}: {e}")
    except Exception as e:
        print(f"\nError reading parquet files: {e}")

if __name__ == "__main__":
    main()
