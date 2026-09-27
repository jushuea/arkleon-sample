# Arklēon sample dataset

This repository is a sample export from the Arklēon data layer: SEC Financial Statement Data Sets facts and revision events for 50 selected companies, published under CC BY 4.0.

**Quick start:** [open the quickstart notebook in Google Colab](https://colab.research.google.com/github/jushuea/arkleon-sample/blob/main/quickstart.ipynb). The notebook downloads the two Parquet files itself when they are not next to it.

## Files

- `facts.parquet`: numeric facts in Parquet form.
- `facts.csv.gz`: the same facts rows as gzip-compressed CSV.
- `revisions.parquet`: revision events in Parquet form.
- `revisions.csv.gz`: the same revision rows as gzip-compressed CSV.
- `README.md`: this file.
- `LICENSE`: the full text of the CC BY 4.0 license.
- `CITATION.cff`: the citation for this dataset.
- `generate.py`: the exporter script.
- `quickstart.ipynb`: the quickstart notebook.
- `field_dictionary.md`: the field dictionary for both tables.
- `snapshot.json`: the snapshot record written by `generate.py`.
- `manifest.json`: the download list used by the Arklēon website.

## Data files

Four data files sit next to this README.

- `facts.parquet` and `facts.csv.gz`. One row per numeric fact, in the /v1/facts row shape. Columns, in export order: `accession_id`, `tag`, `taxonomy`, `period_end`, `duration_quarters`, `unit`, `segments`, `coreg`, `value`, `form`, `filed`, `cik`, `source_url`.
- `revisions.parquet` and `revisions.csv.gz`. One row per revision event. Seven columns carry the /v1/revisions event fields: `val`, `filed`, `accession_id`, `form`, `event_type`, `supersedes_accn`, `superseded_by_accn`. Five further columns identify the series each event belongs to: `cik`, `tag`, `period_end`, `duration_quarters`, `unit`. Those five are sample-export additions. They are not /v1/revisions response fields.

The Parquet and CSV files of each pair hold the same rows in the same column order. CSV dates are ISO `YYYY-MM-DD` text. A CSV empty cell stands for either a null or an empty string. Parquet keeps the two apart. In Parquet, `value` and `val` are floating point.

## The as_of rule

Section 2.1.1 of the API contract states the rule. `/v1/facts` returns only facts whose filing was filed on or before `as_of`. A fact from a filing submitted after `as_of` does not appear in the response, regardless of the period the fact describes. `as_of` filters on the filing date, never on the period. On `/v1/facts` the parameter is required and has no default. A request that omits it is rejected. The API does not substitute today's date.

These files apply no `as_of` cutoff. They carry every filing for the selected companies. To build a point-in-time view, keep the rows whose `filed` value is on or before the chosen date. The `period_end` column plays no part in that cut.

## Selection rule

`snapshot.json` records the rule under `selection_rule`. In plain terms:

- An Assets row is a fact with tag `Assets`, consolidated (`segments` and `coreg` both empty), non-null, instantaneous (`qtrs` = 0), unit USD, in a us-gaap taxonomy (version starting `us-gaap`).
- A CIK's latest reported Assets is its latest such row, ordered by filing date, then `period_end`, then `accession_id`, then taxonomy version, all descending.
- A CIK is active if its latest stored submission was filed on or after 2024-01-01, and inactive otherwise.
- A CIK is eligible if it has at least one row in `edgar_fsds_revision_events`.
- Eligible active CIKs are ordered by Assets ascending, then CIK ascending. The first 20 form the smallest active group and the last 20 form the largest active group. At least 40 are required. The first 10 eligible inactive CIKs by CIK ascending form the inactive group. The run needs 50 distinct CIKs and fails otherwise. The criteria are not relaxed.

The Assets filters choose companies only. They do not filter the extracted facts.

## Selected companies

### Smallest active

| CIK | Company |
|---|---|
| 29952 | CHINA CHANGJIANG MINING & NEW ENERGY COMPANY, LTD. |
| 768216 | COYNI, INC. |
| 790273 | CONECTISYS CORP |
| 803649 | EQC LIQUIDATING TRUST |
| 831378 | LVPAI GROUP LTD |
| 1043150 | ELINE ENTERTAINMENT GROUP, INC. |
| 1062506 | ATLANTICA INC |
| 1093636 | SAXON CAPITAL GROUP, INC./DE |
| 1284454 | NONGFU SHOP DIGITAL NEW RETAIL CO., LTD |
| 1346287 | CLUSTER GROUP HOLDINGS LTD CO |
| 1392449 | GREEN PLANET BIO ENGINEERING CO. LTD. |
| 1410187 | CANNONAU CORP. |
| 1434740 | RANGER GOLD CORP. |
| 1467845 | KASHIN, INC. |
| 1528188 | WORLDWIDE NFT INC. |
| 1580095 | BLACK ROCK PETROLEUM CO |
| 1584480 | GREENTECH INNOVATIONS, INC. |
| 1594968 | LOAN ARTIFICIAL INTELLIGENCE CORP. |
| 1609258 | PETROGAS CO |
| 1619227 | CLOUDWEB, INC. |

### Largest active

| CIK | Company |
|---|---|
| 92230 | TRUIST FINANCIAL CORP |
| 1390777 | BANK OF NEW YORK MELLON CORP |
| 1144967 | HDFC BANK LTD |
| 713676 | PNC FINANCIAL SERVICES GROUP, INC. |
| 927628 | CAPITAL ONE FINANCIAL CORP |
| 789019 | MICROSOFT CORP |
| 36104 | US BANCORP DE |
| 1652044 | ALPHABET INC. |
| 1099219 | METLIFE INC |
| 1137774 | PRUDENTIAL FINANCIAL INC |
| 1018724 | AMAZON COM INC |
| 1067983 | BERKSHIRE HATHAWAY INC |
| 895421 | MORGAN STANLEY |
| 886982 | GOLDMAN SACHS GROUP INC |
| 72971 | WELLS FARGO & COMPANY/MN |
| 831001 | CITIGROUP INC |
| 70858 | BANK OF AMERICA CORP /DE/ |
| 1026214 | FEDERAL HOME LOAN MORTGAGE CORP |
| 310522 | FEDERAL NATIONAL MORTGAGE ASSOCIATION FANNIE MAE |
| 19617 | JPMORGAN CHASE & CO |

### Inactive

| CIK | Company |
|---|---|
| 2034 | ACETO CORP |
| 2491 | BALLY TECHNOLOGIES, INC. |
| 3116 | AKORN INC |
| 3673 | ALLEGHENY ENERGY, INC |
| 3952 | ALLIED DEFENSE GROUP INC |
| 4187 | INDUSTRIAL SERVICES OF AMERICA INC |
| 4515 | AMERICAN AIRLINES INC |
| 4828 | AMERICAN CRYSTAL SUGAR CO /MN/ |
| 4969 | AMERICAN EXPRESS CREDIT CORP |
| 5117 | EMTEC INC/NJ |

## Row counts and sizes

| File | Rows | Bytes |
|---|---|---|
| facts.parquet | 1,548,791 | 23,017,443 |
| facts.csv.gz | 1,548,791 | 17,962,780 |
| revisions.parquet | 15,539 | 349,348 |
| revisions.csv.gz | 15,539 | 202,416 |

The facts rows come from 2,249 filings of the 50 CIKs and cover 4,758 distinct tags. Their filing dates run from 2009-07-24 to 2026-06-29. The revisions rows split into 14,064 RESTATED_COMPARATIVE events and 1,475 AMENDMENT events. Their filing dates run from 2009-11-04 to 2026-06-25. `snapshot.json` records a `sha256` digest for each file.

## Generation

Snapshot id: `sample-dataset-1/20260927T043424Z`

Extraction time: 2026-09-27T04:34:24Z

`generate.py` writes the snapshot record to a file named `manifest.json`, published in this repository as `snapshot.json`, while the repository-root `manifest.json` is a different file: the download list used by the Arklēon website, listing each published file with its byte size plus the Colab link.

The exporter made read-only service reads of three tables: `edgar_fsds_submissions`, `edgar_fsds_facts`, `edgar_fsds_revision_events`. It wrote nothing to the service. `snapshot.json` records the table names and the query parameter templates used for selection and extraction, with a `sha256` digest of each template set.

## Corpus version

No upstream corpus-version identifier exists. `snapshot.json` records a null identifier with a note saying so. The snapshot id above is the identifier for these files.

The certified corpus figures in `certifiedFigures.mjs` cover 2009q1 to 2026q1. That file is in the Arklēon application source, not in this repository. These files include filings dated after that range: facts were filed 2009-07-24 to 2026-06-29. Filings after 2026q1 are outside the certified figures.

## Regenerating

Run `generate.py --out DIR`. The `--out` option is required and names the output directory. The script needs service credentials in the environment variables `SUPABASE_URL` and `SUPABASE_SECRET_KEY`. If `SUPABASE_SECRET_KEY` is unset, it falls back to `SUPABASE_SERVICE_ROLE_KEY`. Those credentials are Arklēon's own, so outside users cannot rerun the extraction, but they can check the four data files against the sha256 digests in `snapshot.json`.

Three further options select a narrower job. They are mutually exclusive, so pass at most one of them:

- `--select-only` writes `selection.json` and nothing else.
- `--ciks N,N` extracts only the listed CIKs.
- `--manifest` computes `manifest.json`, the snapshot record, offline from the existing files, and this repository publishes that record as `snapshot.json`. It also requires `--snapshot-time`, the start of the original extraction, in the form `YYYYMMDDTHHMMSSZ`.

## Field dictionary

`field_dictionary.md` lists every column of both tables in export order, with its Arrow type, its meaning, and the contract section it follows.

## Quickstart notebook

`quickstart.ipynb` loads both Parquet files, selects one CITIGROUP INC series, compares its first filed value with a later reported value, applies the as_of rule locally at two dates, and shows the matching revision events.

## Citation

`CITATION.cff` gives the citation. Cite the snapshot id `sample-dataset-1/20260927T043424Z` as the version.

## License

The data and files in this repository are licensed under the Creative Commons Attribution 4.0 International license (CC BY 4.0). The full text is in `LICENSE`. `CITATION.cff` gives the citation.
