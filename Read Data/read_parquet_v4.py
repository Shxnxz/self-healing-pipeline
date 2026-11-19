#!/usr/bin/env python3
"""Create a printable, offline HTML report for one record in read_parquet_v3.

Run beside read_parquet_v3.py, or supply --inspector /path/to/read_parquet_v3.py.
Requires the same Python dependencies and Delta data as the inspector.
"""
import argparse
import html
import importlib.util
from datetime import datetime
from pathlib import Path

LAYERS = [('bronze', 'Bronze', 'Raw ingestion'), ('silver1', 'Silver 1', 'Cleaned and flattened'),
          ('silver2', 'Silver 2', 'Standardized'), ('gold', 'Gold', 'Business-ready output')]

def esc(value):
    return html.escape(str(value), quote=True)

def present(frame):
    return frame is not None and not frame.empty

def frame_table(frame):
    if not present(frame):
        return '<p class="muted">No matching record was returned for this table.</p>'
    headers = ''.join(f'<th scope="col">{esc(c)}</th>' for c in frame.columns)
    rows = ''.join('<tr>' + ''.join(f'<td>{esc("" if v is None else v)}</td>' for v in row)
                   + '</tr>' for row in frame.itertuples(index=False, name=None))
    return f'<div class="table-wrap"><table><thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table></div>'

def build_report(inspector, record_id, records, source=None):
    branch = inspector.record_branch(records) or source or 'Unknown'
    stages = []
    for key, title, description in LAYERS:
        status = 'Found' if present(records.get(key)) else 'Not found'
        if key == 'silver2' and present(records.get('quarantine')) and not present(records.get(key)):
            status = 'In quarantine'
        stages.append(f'<div class="stage"><span>{esc(title)}</span><strong>{esc(status)}</strong><small>{esc(description)}</small></div>')
    diffs = inspector.compute_layer_diffs(records)
    if diffs:
        diff_rows = ''.join('<tr>' + ''.join(f'<td>{esc(v)}</td>' for v in row) + '</tr>' for row in diffs)
        changes = '<div class="table-wrap"><table><thead><tr><th>Field</th><th>Bronze</th><th>Silver 1</th><th>Silver 2</th><th>Gold</th></tr></thead><tbody>' + diff_rows + '</tbody></table></div>'
    else:
        changes = '<p class="muted">No differing values were identified by the inspector.</p>'
    sections = []
    for key, title, _ in LAYERS:
        sections.append(f'<section><h2>{esc(title)}</h2>{frame_table(records.get(key))}</section>')
    for key, title in [('quarantine', 'Quarantine'), ('audit', 'Fuzzy matching audit')]:
        if present(records.get(key)):
            sections.append(f'<section><h2>{esc(title)}</h2>{frame_table(records[key])}</section>')
    stamp = datetime.now().astimezone().strftime('%Y-%m-%d %H:%M %Z')
    return f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Pipeline flow report</title><style>
*{{box-sizing:border-box}}body{{margin:0;background:#f2f5fa;color:#18273c;font:14px/1.5 system-ui,sans-serif}}main{{max-width:1400px;margin:auto;padding:40px}}h1{{font-size:30px;margin:0 0 8px}}h2{{font-size:20px;margin:0 0 15px}}.meta{{color:#53657a;overflow-wrap:anywhere}}section{{background:white;border:1px solid #dce4ee;border-radius:10px;padding:22px;margin:18px 0;break-inside:avoid}}.flow{{display:grid;grid-template-columns:repeat(4,1fr);gap:12px}}.stage{{background:#eef5ff;border-left:4px solid #346ac3;padding:14px;border-radius:5px}}.stage span,.stage strong,.stage small{{display:block}}.stage strong{{font-size:17px;margin:5px 0}}.stage small,.muted{{color:#53657a}}.table-wrap{{overflow:auto}}table{{width:100%;border-collapse:collapse;font-size:12px}}th,td{{border:1px solid #dce4ee;padding:7px;text-align:left;vertical-align:top;overflow-wrap:anywhere;min-width:95px}}th{{background:#edf2f8}}.note{{font-size:12px;color:#53657a}}@media(max-width:700px){{.flow{{grid-template-columns:1fr 1fr}}main{{padding:16px}}}}@media print{{@page{{size:landscape;margin:12mm}}body{{background:white}}main{{max-width:none;padding:0}}section{{border:0;border-top:1px solid #ccc;border-radius:0;padding:12px 0;break-inside:auto}}.table-wrap{{overflow:visible}}table{{table-layout:fixed;font-size:8px}}th,td{{min-width:0;padding:3px}}thead{{display:table-header-group}}tr{{break-inside:avoid}}}}
</style></head><body><main><header><h1>Pipeline flow report</h1><p class="meta"><strong>Record:</strong> {esc(record_id)} &nbsp; · &nbsp; <strong>Branch:</strong> {esc(branch)} &nbsp; · &nbsp; <strong>Generated:</strong> {esc(stamp)}</p></header><section><h2>Record journey</h2><div class="flow">{''.join(stages)}</div><p class="note">Statuses reflect matching rows returned by the inspector at report generation time. A missing row may also reflect a lookup error or unavailable table.</p></section><section><h2>Field changes</h2>{changes}<p class="note">Changes use the inspector’s comparison of shared fields with different non-empty values; this is not a complete list of added or removed fields.</p></section>{''.join(sections)}<p class="note">Open this file in a browser and print or save as PDF. Wide tables may require browser print scaling.</p></main></body></html>'''

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inspector', type=Path, default=Path(__file__).resolve().with_name('read_parquet_v3.py'), help='Path to read_parquet_v3.py')
    parser.add_argument('--id', required=True, help='Record ID or prefix to trace')
    parser.add_argument('--source', help='Restrict lookups to one source branch, e.g. crash3')
    parser.add_argument('--output', type=Path, default=Path('pipeline_flow_report.html'), help='HTML output path')
    args = parser.parse_args()
    if not args.inspector.is_file():
        parser.error(f'inspector not found: {args.inspector}; specify --inspector')
    try:
        spec = importlib.util.spec_from_file_location('parquet_inspector_v3', args.inspector)
        inspector = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(inspector)
    except ImportError as exc:
        parser.exit(1, f'Missing dependency: {exc}. Install the dependencies required by read_parquet_v3.py.\n')
    if args.source:
        inspector.SOURCE_FILTER = args.source.lower()
        if inspector.SOURCE_FILTER not in inspector.SOURCES:
            parser.error(f'unknown source {args.source!r}; available: {", ".join(inspector.SOURCES)}')
    records = inspector.fetch_record_across_pipeline(args.id)
    if not any(present(frame) for frame in records.values()):
        parser.exit(1, 'No matching record was returned. Check the ID, source, data availability, and inspector configuration.\n')
    report = build_report(inspector, args.id, records, args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding='utf-8')
    print(f'Report written to {args.output.resolve()}')

if __name__ == '__main__':
    main()