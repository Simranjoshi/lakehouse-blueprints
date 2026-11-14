# Cluster and Spark notes

These are the knobs that produced the 70% cost cut and 80% runtime cut on large batch jobs in production. Encoded here as practice, not as a dump of workplace configs.

1. Partition on the column the jobs actually filter, not on a high-cardinality ID.
2. Size the cluster to the shuffle, then turn autoscaling off for stable SLA jobs.
3. Cache only reuse points. Caching bronze just to “feel faster” burned DBU.
4. Broadcast small dimension tables; do not broadcast a slowly growing conformed table.
5. Inspect scan, shuffle, spill, and skew in Spark UI before adding executors.
6. Prefer Delta `OPTIMIZE` + `ZORDER` on gold read paths over extra cluster memory.
7. Fail the job in `quality/checks.py` when grain or freshness breaks. Silent gold tables are more expensive than a red job.
