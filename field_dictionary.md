# SAMPLE-DATASET-1 field dictionary

Types below are Arrow types stored in the Parquet files, taken from the explicit
schemas in `extract.py`. The corresponding `facts.csv.gz` and `revisions.csv.gz`
use exactly the same field order, with dates as ISO `YYYY-MM-DD` text. CSV empty
cells represent nulls or empty strings; Parquet preserves that distinction.
All Parquet fields are nullable. Arrow `double` is 64-bit floating point:
`value` and `val` are floating point, while the /v1 wire value is a JSON number.
No decimal or integer precision beyond the source extraction's float conversion
is promised.

Contract citations refer to
the Arklēon API v1 contract
and the Arklēon revision contract (September 2026).
The API contract specifies the vocabulary and meanings; the sample extraction
specifies the Parquet storage types.

## facts.*

| Field, in export order | Arrow type | Meaning and contract section |
|---|---|---|
| `accession_id` | `string` | SEC accession number identifying the exact filing the fact came from; FSDS `adsh`. API §§3.1, 3.1.1. |
| `tag` | `string` | As-filed fact tag, mapped from facts `tag`. API §3.1.1. |
| `taxonomy` | `string` | Taxonomy version, mapped from facts `version`. API §3.1.1. |
| `period_end` | `date32[day]` | End date of the period the fact describes, mapped from `ddate`; distinct from the filing date. API §§3.1.1, 2.1.1. |
| `duration_quarters` | `int32` | Fact duration in quarters, mapped from `qtrs`. Zero denotes an instantaneous fact, as used by the selection rule. API §3.1.1 mapping; `selection_query.py` selection rule. |
| `unit` | `string` | Fact unit of measure, mapped from `uom`, preserved as stored. API §3.1.1. |
| `segments` | `string` | FSDS segment designation, mapped unchanged from `segments`. Empty string is part of the consolidated-fact definition. API §3.1.1; revision contract §D1. |
| `coreg` | `string` | FSDS coregistrant designation, mapped unchanged from `coreg`. Empty string is part of the consolidated-fact definition. API §3.1.1; revision contract §D1. |
| `value` | `double` | Numeric fact value mapped from facts `value`, converted to floating point by the sample extraction; null remains null. API §3.1.1 mapping. |
| `form` | `string` | Filing form type, resolved from the submissions record joined by accession. API §§3.1, 3.1.1; examples in §2.5.2 include `10-K`, `10-Q`, `10-K/A`. |
| `filed` | `date32[day]` | Filing date from that submissions record. This is the point-in-time driver: `as_of` constrains the filing date, never the period described. API §§2.1.1, 3.1, 3.1.1. |
| `cik` | `int64` | Company identity (SEC Central Index Key), resolved from submissions by accession. API §§3.1, 3.1.1. |
| `source_url` | `string` | Computed SEC EDGAR filing index URL derived solely from accession and CIK, rather than a stored URL. API §3.1.1. |

The URL construction, matching `v1Handlers.mjs` lines 187-193, is
`https://www.sec.gov/Archives/edgar/data/{integer_cik}/{accession_without_hyphens}/{accession_id}-index.htm`.

This sample retains all fact tags and periods for the selected companies'
submissions with a nonempty `filed` date, as `extract.py` does. It applies no
`as_of` cutoff. The consolidated Assets filters determine company selection;
they do not filter the extracted facts. For point-in-time use, apply the
filed-date bound described in API §2.1.1.

## revisions.*

| Field, in export order | Arrow type | Meaning and contract section |
|---|---|---|
| `cik` | `int64` | sample-export addition: series identity, not a /v1/revisions response field. Company identity in the series key, obtained through the filing join when the event is built. Revision contract §D1; API §3.1.1. |
| `tag` | `string` | sample-export addition: series identity, not a /v1/revisions response field. Fact tag in the series key. Revision contract §D1; API §3.1.1. |
| `period_end` | `date32[day]` | sample-export addition: series identity, not a /v1/revisions response field. Series period end (`ddate`). Revision contract §D1; API §3.1.1. |
| `duration_quarters` | `int32` | sample-export addition: series identity, not a /v1/revisions response field. Series duration in quarters (`qtrs`). Revision contract §D1; API §3.1.1. |
| `unit` | `string` | sample-export addition: series identity, not a /v1/revisions response field. Series unit of measure (`uom`). Revision contract §D1; API §3.1.1. |
| `val` | `double` | Numeric value recorded by the filing that produced the event, converted to floating point; null remains null. API §2.5.2; `v1Handlers.mjs` line 557. |
| `filed` | `date32[day]` | Filing date of the event's filing, with the same point-in-time meaning as facts `filed`. API §§2.5.2, 2.1.1. |
| `accession_id` | `string` | Accession of the filing that produced the event. API §2.5.2. |
| `form` | `string` | Filing form type, such as `10-K`, `10-Q`, or `10-K/A`. API §2.5.2. |
| `event_type` | `string` | Revision-event classification: `AMENDMENT` or `RESTATED_COMPARATIVE`. These name event types on corpus rows, not characterizations of the filing. API §2.5.2. |
| `supersedes_accn` | `string` | Accession that this event's filing superseded, or null; emitted from `prior_adsh`. API §2.5.2; `v1Handlers.mjs` line 562. |
| `superseded_by_accn` | `string` | Accession that later superseded this event's filing, or null; computed from the next event in the same series ordered by `seq`, then accession. API §2.5.2; `v1Handlers.mjs` lines 539-548; `extract.py`. |

Revision series identity is `(cik, tag, ddate, qtrs, uom)` over consolidated
facts with `segments = ''`, `coreg = ''`, non-null value, and
`ddate <= current_date`; taxonomy `version` is excluded (revision contract
§D1). These are upstream event-building semantics. The exporter reads the
stored events; it does not rebuild or filter the event store. The five added
API-named identity columns let readers distinguish series in the flat sample.
The seven remaining columns follow API §2.5.2 and the response object in
`v1Handlers.mjs` lines 556-564.
