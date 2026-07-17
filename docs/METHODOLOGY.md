# Methodology and reproducibility notes

## Unit of analysis

The collection target is a public GitHub organization. The `organization` endpoint is used to verify the account type. Repository ownership is defined by the organization namespace at collection time. This is not, by itself, proof that the organization corresponds to a specific legal firm; researchers should retain their external entity-matching table and matching evidence.

## Innovation proxies

- Repository creation is a count of repositories with `created_at < cutoff`. `is_fork=false` is an original repository and `is_fork=true` is a fork. A repository can still be trivial, empty, mirrored, or generated; robustness screens may be appropriate.
- Code production is a commit count across collected organization repositories. The baseline uses the best available timestamp before the cutoff.
- Community contribution uses non-pull-request issue creation.
- Collaborative code development uses pull-request creation, plus optional merge/review/change-volume fields.

The CLI uses strict `< cutoff` comparisons, matching “before the fundraising conclusion.” Change to `<=` only if the research design defines the event instant as inclusive.

## Recommended panel workflow

1. Freeze an organization-to-firm crosswalk with match confidence and evidence date.
2. Define a common start date and observation/fundraising cutoffs in UTC.
3. Collect organization/repository state and complete reachable commit history.
4. Ingest GH Archive in bounded ranges for public historical events and cross-check gaps.
5. Record every collection run and retain software version/configuration.
6. Construct measures with SQL at the cutoff, not by using today’s state for historical counts.
7. Report missingness separately from structural zero.

## Stars and forks at historical cutoffs

GitHub’s API exposes current counts. A snapshot collected today must not be treated as the number of stars or forks at a past fundraising cutoff. Build a forward-looking panel with `repository_metric_snapshot`, or reconstruct historical star/fork events from GH Archive with explicit coverage limitations.

## Python dependency screen

The parser intentionally handles only literal `install_requires` lists or literal lists assigned to a variable and passed to `setup()`. It does not execute code, resolve `requirements.txt`, expand environment markers, or infer dynamically generated requirements. This conservative rule is reproducible and safe but produces documented false negatives.

