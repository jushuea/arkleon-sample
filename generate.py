"""Reproduce SAMPLE-DATASET-1 using sequential PostgREST GET reads only.

Dependencies: requests, pyarrow. Credentials come only from SUPABASE_URL and
SUPABASE_SECRET_KEY (falling back to SUPABASE_SERVICE_ROLE_KEY).

Selection rule (directive step 1 as folded by the astra verifier, FOLD 1):
  Assets                      consolidated (segments = '' and coreg = ''),
                              non-null, instantaneous (qtrs = 0), USD,
                              us-gaap taxonomy (version starts 'us-gaap'),
                              tag = 'Assets'
  latest reported Assets      the CIK's latest such row ordered by filing date,
                              then period_end, then accession_id, then taxonomy
                              version, all descending
  activity                    the CIK's latest stored submission filed date:
                              on or after 2024-01-01 active, before it inactive
  eligible                    at least one edgar_fsds_revision_events row
  groups                      eligible active CIKs ordered by Assets ascending,
                              then CIK ascending: the first 20 and the last 20
                              (at least 40 required); the first 10 eligible
                              inactive CIKs by CIK ascending; 50 distinct CIKs
                              or the run fails without relaxing the criteria

Full runs write selection.json, the submissions snapshot, facts and revisions
in Parquet and gzip CSV, extract_summary.json, and manifest.json. --select-only
writes selection.json only. --ciks bypasses selection and extracts the given
CIKs after the same submissions scan. --manifest is entirely offline and
requires --snapshot-time to identify the original extraction start.
"""
import argparse
import csv
import gzip
import hashlib
import json
import os
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

import requests
import pyarrow as pa
import pyarrow.parquet as pq

PAGE = 1000
ACTIVE_CUTOFF = '2024-01-01'
ASSETS_BATCH = 150
FACT_BATCH = 1

SELECTION_RULE = "  Assets                      consolidated (segments = '' and coreg = ''),\n                              non-null, instantaneous (qtrs = 0), USD,\n                              us-gaap taxonomy (version starts 'us-gaap'),\n                              tag = 'Assets'\n  latest reported Assets      the CIK's latest such row ordered by filing date,\n                              then period_end, then accession_id, then taxonomy\n                              version, all descending\n  activity                    the CIK's latest stored submission filed date:\n                              on or after 2024-01-01 active, before it inactive\n  eligible                    at least one edgar_fsds_revision_events row\n  groups                      eligible active CIKs ordered by Assets ascending,\n                              then CIK ascending: the first 20 and the last 20\n                              (at least 40 required); the first 10 eligible\n                              inactive CIKs by CIK ascending; 50 distinct CIKs\n                              or the run fails without relaxing the criteria"



# Hash these exact parameter templates as canonical JSON. Dynamic bindings are
# explicit: historical concrete requests cannot be recovered from data files.
# Every outgoing query is built from one of these templates.
QUERY_PARAMETER_SETS = {
    'selection': {
        'submissions_first': {
            'table': 'edgar_fsds_submissions',
            'params': {'select': 'adsh,cik,name,form,filed,period,fy,fp',
                       'order': 'adsh.asc', 'limit': PAGE},
        },
        'submissions_next': {
            'table': 'edgar_fsds_submissions',
            'params': {'select': 'adsh,cik,name,form,filed,period,fy,fp',
                       'order': 'adsh.asc', 'limit': PAGE,
                       'adsh': '${after_accession_filter}'},
        },
        'assets': {
            'table': 'edgar_fsds_facts',
            'params': {
                'select': 'adsh,tag,version,ddate,qtrs,uom,segments,coreg,value',
                'adsh': '${accession_list_filter}', 'tag': 'eq.Assets',
                'uom': 'eq.USD', 'qtrs': 'eq.0', 'segments': 'eq.',
                'coreg': 'eq.', 'limit': PAGE,
            },
        },
        'revision_eligibility': {
            'table': 'edgar_fsds_revision_events',
            'params': {'select': 'cik', 'cik': '${cik_filter}', 'limit': 1},
        },
    },
    'extraction': {
        'facts': {
            'table': 'edgar_fsds_facts',
            'params': {
                'select': 'adsh,tag,version,ddate,qtrs,uom,segments,coreg,value',
                'adsh': '${accession_filter}',
                'order': 'adsh.asc,tag.asc,version.asc,ddate.asc,qtrs.asc,uom.asc,segments.asc,coreg.asc',
                'limit': PAGE, 'offset': '${offset}',
            },
        },
        'revisions': {
            'table': 'edgar_fsds_revision_events',
            'params': {
                'select': 'cik,tag,ddate,qtrs,uom,seq,adsh,form,filed,value,prior_adsh,event_type',
                'cik': '${cik_filter}',
                'order': 'cik.asc,tag.asc,ddate.asc,qtrs.asc,uom.asc,adsh.asc',
                'limit': PAGE, 'offset': '${offset}',
            },
        },
    },
}
# --ciks uses this same scan to resolve submissions lineage without selection.
for _name in ('submissions_first', 'submissions_next'):
    QUERY_PARAMETER_SETS['extraction'][_name] = QUERY_PARAMETER_SETS['selection'][_name]


def query_params(phase, name, **bindings):
    params = QUERY_PARAMETER_SETS[phase][name]['params']
    return {key: bindings[value[2:-1]] if isinstance(value, str)
            and value.startswith('${') and value.endswith('}') else value
            for key, value in params.items()}


class Rest:
    def __init__(self):
        base = os.environ.get('SUPABASE_URL')
        key = (os.environ.get('SUPABASE_SECRET_KEY')
               or os.environ.get('SUPABASE_SERVICE_ROLE_KEY'))
        if not base or not key:
            raise RuntimeError('Required Supabase environment variables are missing')
        self.base = base.rstrip('/') + '/rest/v1/'
        self._h = {'apikey': key, 'Authorization': f'Bearer {key}',
                   'Accept': 'application/json'}
        self.requests = 0

    def get(self, table, params, count=False, retries=5):
        headers = dict(self._h)
        if count:
            headers['Prefer'] = 'count=exact'
        for attempt in range(retries):
            try:
                response = requests.get(self.base + table, params=params,
                                        headers=headers, timeout=120)
                if response.status_code in (200, 206):
                    self.requests += 1
                    if count:
                        return response.json(), response.headers.get('content-range')
                    return response.json()
                # Response bodies and exception messages can contain secrets.
                err = f'HTTP {response.status_code}'
            except requests.RequestException as exc:
                err = type(exc).__name__
            time.sleep(2 * (attempt + 1))
        raise RuntimeError(f'GET {table} failed after {retries} attempts: {err}')



def in_list(values):
    """PostgREST in.() list with every value double-quoted."""
    return 'in.(' + ','.join('"' + str(v).replace('"', '') + '"' for v in values) + ')'


def scan_submissions(rest, out_path=None):
    """Keyset scan of every submissions row in adsh order."""
    rows, last = [], None
    t0 = time.time()
    while True:
        params = query_params(
            'selection', 'submissions_first' if last is None else 'submissions_next',
            after_accession_filter=f'gt.{last}')
        page = rest.get('edgar_fsds_submissions', params)
        rows.extend(page)
        if len(page) < PAGE:
            break
        last = page[-1]['adsh']
        if rest.requests % 50 == 0:
            print(f'  submissions {len(rows):,} rows, {time.time() - t0:.0f}s', flush=True)
    if out_path is not None:
        with open(out_path, 'w') as fh:
            for r in rows:
                fh.write(json.dumps(r, separators=(',', ':')) + '\n')
    return rows


def filing_order_key(sub):
    # Newest first: filed DESC, amendment after base on the same filed date
    # (so the amendment is "later"), then accession DESC.
    form = sub.get('form') or ''
    return (sub['filed'] or '', 1 if form.endswith('/A') else 0, sub['adsh'])


def pick_assets(rows):
    """From one filing's Assets rows: us-gaap only, latest period_end, then
    larger taxonomy version."""
    rows = [r for r in rows if r['value'] is not None and str(r['version']).startswith('us-gaap')]
    if not rows:
        return None
    return max(rows, key=lambda r: (r['ddate'], r['version']))


def latest_assets(rest, filings_by_cik, ciks, label):
    """Walk each CIK's filings one filed date at a time, newest first, until a
    date's filings carry Assets. Among that date's filings: latest period_end,
    then accession, then taxonomy version, all descending (FOLD 1 rule)."""
    queues = {}
    for c in ciks:
        by_date = defaultdict(list)
        for s in filings_by_cik[c]:
            by_date[s['filed']].append(s)
        queues[c] = [by_date[k] for k in sorted(by_date, reverse=True)]
    found, rnd = {}, 0
    pending = {c for c in ciks if queues[c]}
    while pending:
        rnd += 1
        head = {}
        for c in pending:
            for s in queues[c][0]:
                head[s['adsh']] = (c, s)
        adshs = sorted(head)
        got = defaultdict(list)
        for i in range(0, len(adshs), ASSETS_BATCH):
            batch = adshs[i:i + ASSETS_BATCH]
            params = query_params('selection', 'assets',
                                  accession_list_filter=in_list(batch))
            page = rest.get('edgar_fsds_facts', params)
            if len(page) >= PAGE:
                raise RuntimeError('Assets batch hit the page cap; lower ASSETS_BATCH')
            for r in page:
                got[r['adsh']].append(r)
        nxt = set()
        for c in pending:
            day = queues[c].pop(0)
            picks = []
            for s in day:
                pick = pick_assets(got.get(s['adsh'], []))
                if pick is not None:
                    picks.append((pick['ddate'], s['adsh'], pick['version'], pick, s))
            if picks:
                picks.sort(key=lambda x: (x[0], x[1], x[2]), reverse=True)
                _, adsh, _, pick, sub = picks[0]
                found[c] = {'value': float(pick['value']), 'period_end': pick['ddate'],
                            'taxonomy': pick['version'], 'accession_id': adsh,
                            'form': sub.get('form'), 'filed': sub['filed']}
            elif queues[c]:
                nxt.add(c)
        print(f'  {label} round {rnd}: {len(head)} filings probed, {len(found):,} CIKs resolved, '
              f'{len(nxt):,} pending', flush=True)
        pending = nxt
    return found


def has_revision(rest, cik):
    page = rest.get('edgar_fsds_revision_events', query_params('selection', 'revision_eligibility', cik_filter=f'eq.{cik}'))
    return len(page) > 0


def select_companies(rest, subs, out):
    by_cik = defaultdict(list)
    for s in subs:
        if s['filed']:
            by_cik[s['cik']].append(s)
    latest = {c: max(x['filed'] for x in v) for c, v in by_cik.items()}
    name = {}
    for c, v in by_cik.items():
        name[c] = max(v, key=filing_order_key).get('name')
    active = sorted(c for c, d in latest.items() if d >= ACTIVE_CUTOFF)
    inactive = sorted(c for c, d in latest.items() if d < ACTIVE_CUTOFF)
    null_filed = sum(1 for s in subs if not s['filed'])
    print(f'  submissions rows {len(subs):,}; distinct CIKs {len(by_cik):,}; null filed {null_filed}; '
          f'active {len(active):,}; inactive {len(inactive):,}', flush=True)

    print('latest reported Assets (active CIKs)', flush=True)
    a_act = latest_assets(rest, by_cik, active, 'active')

    # Eligible active CIKs ordered by Assets ascending, then CIK ascending.
    asc = sorted(a_act.items(), key=lambda kv: (kv[1]['value'], kv[0]))
    print('revision-event eligibility', flush=True)
    probed = {}

    def eligible(cik):
        if cik not in probed:
            probed[cik] = has_revision(rest, cik)
        return probed[cik]

    first, i = [], 0
    while len(first) < 20 and i < len(asc):
        if eligible(asc[i][0]):
            first.append(asc[i])
        i += 1
    last, j = [], len(asc) - 1
    while len(last) < 20 and j >= i:
        if eligible(asc[j][0]):
            last.append(asc[j])
        j -= 1
    if len(first) < 20 or len(last) < 20:
        raise RuntimeError(f'fewer than 40 eligible active CIKs (first {len(first)}, last {len(last)})')
    last.reverse()  # keep ascending order: the last 20 of the eligible ascending list
    ina = []
    for cik in inactive:  # already CIK ascending
        if eligible(cik):
            ina.append((cik, None))
            if len(ina) == 10:
                break
    if len(ina) < 10:
        raise RuntimeError(f'only {len(ina)} eligible inactive CIKs')
    skipped = sum(1 for v in probed.values() if not v)
    print(f'  probed {len(probed):,} CIKs, {skipped:,} without revision events', flush=True)

    selection = []
    for group, rows in (('smallest_active', first), ('largest_active', last), ('inactive', ina)):
        for pos, (cik, info) in enumerate(rows, 1):
            info = info or {}
            selection.append({'group': group, 'position': pos, 'cik': cik, 'company_name': name[cik],
                              'latest_filed': latest[cik], 'assets_usd': info.get('value'),
                              'assets_period_end': info.get('period_end'),
                              'assets_taxonomy': info.get('taxonomy'),
                              'assets_accession_id': info.get('accession_id'),
                              'assets_form': info.get('form'), 'assets_filed': info.get('filed')})
    ciks = [r['cik'] for r in selection]
    assert len(ciks) == 50 and len(set(ciks)) == 50, 'selection must be 50 distinct CIKs'
    summary = {
        'submissions_rows': len(subs), 'distinct_ciks': len(by_cik), 'null_filed_rows': null_filed,
        'active_ciks': len(active), 'inactive_ciks': len(inactive),
        'active_with_assets': len(a_act), 'revision_probes': len(probed),
        'probed_without_revision_events': skipped,
        'requests': rest.requests,
    }
    with open(os.path.join(out, 'selection.json'), 'w') as fh:
        json.dump({'summary': summary, 'selection': selection}, fh, indent=1)
    return ciks


FACT_SCHEMA = pa.schema([
    ('accession_id', pa.string()), ('tag', pa.string()), ('taxonomy', pa.string()),
    ('period_end', pa.date32()), ('duration_quarters', pa.int32()), ('unit', pa.string()),
    ('segments', pa.string()), ('coreg', pa.string()), ('value', pa.float64()),
    ('form', pa.string()), ('filed', pa.date32()), ('cik', pa.int64()), ('source_url', pa.string()),
])
REV_SCHEMA = pa.schema([
    ('cik', pa.int64()), ('tag', pa.string()), ('period_end', pa.date32()),
    ('duration_quarters', pa.int32()), ('unit', pa.string()),
    ('val', pa.float64()), ('filed', pa.date32()), ('accession_id', pa.string()),
    ('form', pa.string()), ('event_type', pa.string()),
    ('supersedes_accn', pa.string()), ('superseded_by_accn', pa.string()),
])


def source_url(adsh, cik):
    # v1Handlers.mjs buildSourceUrl
    return f'https://www.sec.gov/Archives/edgar/data/{int(cik)}/{adsh.replace("-", "")}/{adsh}-index.htm'


def num(v):
    return None if v is None else float(v)


def d(v):
    return None if v is None else date.fromisoformat(v)


def csv_cell(v):
    if v is None:
        return ''
    if isinstance(v, float):
        if v.is_integer() and abs(v) < 1e17:
            return str(int(v))
        return repr(v)
    return str(v)


class Sink:
    def __init__(self, parquet_path, csv_path, schema):
        self.schema = schema
        self.pw = pq.ParquetWriter(parquet_path, schema, compression='zstd')
        self.gz = gzip.open(csv_path, 'wt', newline='', compresslevel=6)
        self.cw = csv.writer(self.gz)
        self.cw.writerow(schema.names)
        self.rows = 0

    def write(self, rows):
        if not rows:
            return
        cols = {n: [r[n] for r in rows] for n in self.schema.names}
        self.pw.write_table(pa.Table.from_pydict(cols, schema=self.schema))
        for r in rows:
            self.cw.writerow([csv_cell(r[n]) for n in self.schema.names])
        self.rows += len(rows)

    def close(self):
        self.pw.close()
        self.gz.close()


def paged(rest, table, params):
    """Offset pages over a bounded, PK-ordered result; count-checked."""
    out, offset, expected = [], 0, None
    while True:
        p = dict(params, limit=PAGE, offset=offset)
        if expected is None:
            page, rng = rest.get(table, p, count=True)
            expected = int(rng.split('/')[-1])
        else:
            page = rest.get(table, p)
        out.extend(page)
        if len(page) < PAGE:
            break
        offset += PAGE
    if len(out) != expected:
        raise RuntimeError(f'{table} fetched {len(out)} rows, count=exact said {expected}')
    return out


def extract_companies(rest, subs, ciks, out):
    cikset = set(ciks)
    lineage = {}
    for s in subs:
        if s['cik'] in cikset and s['filed']:
            lineage[s['adsh']] = s
    accessions = sorted(lineage)
    print(f'{len(cikset)} CIKs, {len(accessions):,} filings', flush=True)

    os.makedirs(out, exist_ok=True)
    facts = Sink(os.path.join(out, 'facts.parquet'), os.path.join(out, 'facts.csv.gz'), FACT_SCHEMA)
    per_cik_facts = defaultdict(int)
    t0 = time.time()
    for i in range(0, len(accessions), FACT_BATCH):
        batch = accessions[i:i + FACT_BATCH]
        rows = paged(rest, 'edgar_fsds_facts', query_params(
            'extraction', 'facts', offset=0,
            accession_filter=f'eq.{batch[0]}' if len(batch) == 1 else in_list(batch)))
        shaped = []
        for r in rows:
            s = lineage[r['adsh']]
            shaped.append({
                'accession_id': r['adsh'], 'tag': r['tag'], 'taxonomy': r['version'],
                'period_end': d(r['ddate']), 'duration_quarters': r['qtrs'], 'unit': r['uom'],
                'segments': r['segments'], 'coreg': r['coreg'], 'value': num(r['value']),
                'form': s.get('form'), 'filed': d(s['filed']), 'cik': s['cik'],
                'source_url': source_url(r['adsh'], s['cik']),
            })
            per_cik_facts[s['cik']] += 1
        facts.write(shaped)
        done = min(i + FACT_BATCH, len(accessions))
        if (i // FACT_BATCH) % 100 == 0 or done == len(accessions):
            print(f'  facts: {done:,}/{len(accessions):,} filings, {facts.rows:,} rows, '
                  f'{time.time() - t0:.0f}s', flush=True)
    facts.close()

    revs = Sink(os.path.join(out, 'revisions.parquet'), os.path.join(out, 'revisions.csv.gz'), REV_SCHEMA)
    per_cik_revs = {}
    for cik in sorted(cikset):
        rows = paged(rest, 'edgar_fsds_revision_events', query_params(
            'extraction', 'revisions', offset=0, cik_filter=f'eq.{cik}'))
        # superseded_by_accn: next event's adsh in the same series by seq
        # (v1Handlers.mjs:539-548).
        series = defaultdict(list)
        for r in rows:
            series[(r['cik'], r['tag'], r['ddate'], r['qtrs'], r['uom'])].append(r)
        nxt = {}
        for evs in series.values():
            evs.sort(key=lambda r: (r['seq'], r['adsh']))
            for k, r in enumerate(evs):
                key = (r['cik'], r['tag'], r['ddate'], r['qtrs'], r['uom'], r['adsh'])
                nxt[key] = evs[k + 1]['adsh'] if k + 1 < len(evs) else None
        # Output order is the API's compareRevisionKey order (v1Handlers.mjs:550):
        # cik, tag, ddate, qtrs, uom, adsh, strings by code point as in JS.
        rows.sort(key=lambda r: (r['cik'], r['tag'], r['ddate'], r['qtrs'], r['uom'], r['adsh']))
        shaped = [{
            'cik': r['cik'], 'tag': r['tag'], 'period_end': d(r['ddate']),
            'duration_quarters': r['qtrs'], 'unit': r['uom'],
            'val': num(r['value']), 'filed': d(r['filed']), 'accession_id': r['adsh'],
            'form': r['form'], 'event_type': r['event_type'],
            'supersedes_accn': r['prior_adsh'],
            'superseded_by_accn': nxt[(r['cik'], r['tag'], r['ddate'], r['qtrs'], r['uom'], r['adsh'])],
        } for r in rows]
        revs.write(shaped)
        per_cik_revs[cik] = len(shaped)
        print(f'  revisions cik {cik}: {len(shaped):,}', flush=True)
    revs.close()

    summary = {'facts_rows': facts.rows, 'revisions_rows': revs.rows, 'filings': len(accessions),
               'per_cik_facts': dict(per_cik_facts), 'per_cik_revisions': per_cik_revs,
               'requests': rest.requests, 'seconds': round(time.time() - t0)}
    with open(os.path.join(out, 'extract_summary.json'), 'w') as fh:
        json.dump(summary, fh, indent=1)
    print(json.dumps({k: v for k, v in summary.items() if not k.startswith('per_')}, indent=1))



def canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def snapshot_time(value):
    """Accept the snapshot ID timestamp or an ISO-8601 UTC timestamp."""
    try:
        if re.fullmatch(r'\d{8}T\d{6}Z', value):
            return datetime.strptime(value, '%Y%m%dT%H%M%SZ').replace(tzinfo=timezone.utc)
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.utcoffset() is None or parsed.utcoffset().total_seconds() != 0:
            raise ValueError
        return parsed.astimezone(timezone.utc).replace(microsecond=0)
    except ValueError:
        raise argparse.ArgumentTypeError(
            'snapshot time must be YYYYMMDDTHHMMSSZ or an ISO-8601 UTC timestamp') from None


def cik_list(value):
    try:
        values = value.split(',')
        if not all(re.fullmatch(r'[0-9]+', v) for v in values):
            raise ValueError
        ciks = [int(v) for v in values]
        if not all(c > 0 for c in ciks):
            raise ValueError
        return ciks
    except ValueError:
        raise argparse.ArgumentTypeError('CIKs must be comma-separated positive integers') from None


def write_manifest(out, started, ciks=None):
    """Inspect only existing files. No Rest instance or credentials are used."""
    out = Path(out)
    files = {}
    for stem, schema in (('facts', FACT_SCHEMA), ('revisions', REV_SCHEMA)):
        parquet_path = out / (stem + '.parquet')
        with pq.ParquetFile(parquet_path) as pf:
            actual_schema = pf.schema_arrow
            if not actual_schema.equals(schema, check_metadata=False):
                raise RuntimeError(f'{parquet_path.name} has an unexpected Arrow schema')
            parquet_rows = pf.metadata.num_rows
        columns = [{'name': field.name, 'type': str(field.type)}
                   for field in actual_schema]
        csv_path = out / (stem + '.csv.gz')
        csv_rows = 0
        with gzip.open(csv_path, 'rt', newline='') as fh:
            reader = csv.reader(fh)
            if next(reader, None) != schema.names:
                raise RuntimeError(f'{csv_path.name} has an unexpected header')
            for row in reader:
                if len(row) != len(schema):
                    raise RuntimeError(f'{csv_path.name} has an unexpected row width')
                csv_rows += 1
        if csv_rows != parquet_rows:
            raise RuntimeError(f'{stem}: CSV and Parquet row counts differ')
        for path, rows in ((parquet_path, parquet_rows), (csv_path, csv_rows)):
            files[path.name] = {
                'rows': rows, 'columns': columns, 'bytes': path.stat().st_size,
                'sha256': file_sha256(path),
            }
    query_hashes = {phase: hashlib.sha256(canonical_json(params)).hexdigest()
                    for phase, params in QUERY_PARAMETER_SETS.items()}
    manifest = {
        'snapshot_id': 'sample-dataset-1/' + started.strftime('%Y%m%dT%H%M%SZ'),
        'extraction_time_utc': started.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'source_tables': ['edgar_fsds_submissions', 'edgar_fsds_facts',
                          'edgar_fsds_revision_events'],
        'selection_rule': SELECTION_RULE,
        'upstream_corpus_version_identifier': None,
        'upstream_corpus_version_note': 'No upstream corpus-version identifier exists.',
        'query_parameter_sets': QUERY_PARAMETER_SETS,
        'query_parameter_sets_sha256': query_hashes,
        'query_hash_scope': (
            'Canonical JSON of the table names and exact query parameter templates '
            'used by this script, not a historical log of concrete requests. '
            'UTF-8, sorted object keys, compact separators, no trailing newline. '
            'Dynamic bindings: after_accession_filter = gt.<last adsh>; '
            'accession_list_filter = in.(double-quoted sorted accession batch); '
            'accession_filter = eq.<accession> for FACT_BATCH=1; '
            'cik_filter = eq.<integer CIK>; offset = integer multiples of PAGE. '
            'The first page of each extraction batch requests Prefer: count=exact. '
            'Submissions scans use keyset pagination; extraction uses offset pages.'),
        'columns_note': 'CSV columns report the corresponding Parquet Arrow types; CSV stores text.',
        'data_files': files,
    }
    if ciks is not None:
        manifest['extraction_ciks_override'] = ciks
        manifest['selection_rule_applied'] = False
    selection_path = out / 'selection.json'
    if selection_path.is_file():
        manifest['selection_file'] = {
            'bytes': selection_path.stat().st_size, 'sha256': file_sha256(selection_path),
        }
    with open(out / 'manifest.json', 'w', encoding='utf-8') as fh:
        json.dump(manifest, fh, indent=2, ensure_ascii=False, allow_nan=False)
        fh.write('\n')


def main():
    started = datetime.now(timezone.utc).replace(microsecond=0)
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--out', required=True, help='output directory')
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument('--select-only', action='store_true', help='write selection.json only')
    mode.add_argument('--ciks', type=cik_list, help='extract only these comma-separated CIKs')
    mode.add_argument('--manifest', action='store_true', help='offline manifest of existing files')
    ap.add_argument('--snapshot-time', type=snapshot_time,
                    help='original UTC extraction start; required with --manifest')
    args = ap.parse_args()
    if args.manifest:
        if args.snapshot_time is None:
            ap.error('--manifest requires --snapshot-time')
        write_manifest(args.out, args.snapshot_time)
        return
    if args.snapshot_time is not None:
        ap.error('--snapshot-time is only valid with --manifest')
    os.makedirs(args.out, exist_ok=True)
    rest = Rest()
    snapshot = None if args.select_only else os.path.join(args.out, 'submissions_snapshot.jsonl')
    print('scan edgar_fsds_submissions', flush=True)
    subs = scan_submissions(rest, snapshot)
    ciks = args.ciks if args.ciks is not None else select_companies(rest, subs, args.out)
    if args.select_only:
        return
    # The original extractor creates a new Rest instance, so its request count
    # excludes the submissions/selection scan. Reuse the connection settings but
    # reset this counter to preserve extract_summary.json's meaning.
    rest.requests = 0
    extract_companies(rest, subs, ciks, args.out)
    write_manifest(args.out, started, args.ciks)


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        # Never emit exception text or a traceback: third-party errors can echo
        # request URLs or credentials. All failures remain nonzero exits.
        print(f'Generation failed ({type(exc).__name__}); no credentials logged.', file=sys.stderr)
        sys.exit(1)
