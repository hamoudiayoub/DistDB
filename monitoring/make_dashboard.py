"""Writes monitoring/grafana/dashboards/distdb.json, the DistDB dashboard Grafana loads at start.

Run it again after changing the panels below: python monitoring/make_dashboard.py
"""
import json
import pathlib

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
next_id = [1]


def place(panel, x, y, w, h):
    panel["gridPos"] = {"x": x, "y": y, "w": w, "h": h}
    panel["id"] = next_id[0]
    next_id[0] += 1
    panels.append(panel)


def target(expr, legend, ref="A", instant=False):
    return {
        "datasource": DS,
        "expr": expr,
        "legendFormat": legend,
        "refId": ref,
        "range": not instant,
        "instant": instant,
    }


def stat(title, expr, unit, x, w, legend="", decimals=None, color="blue", instant=False):
    p = {
        "type": "stat",
        "title": title,
        "datasource": DS,
        "targets": [target(expr, legend, instant=instant)],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "fixed", "fixedColor": color},
                **({"decimals": decimals} if decimals is not None else {}),
            },
            "overrides": [],
        },
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": "value",
            "graphMode": "area",
            "textMode": ("name" if instant else "value_and_name") if legend else "value",
            "justifyMode": "center",
        },
    }
    place(p, x, 0, w, 4)


def series(title, targets, unit, x, y, w=12, h=8, description="", stack=False, signed=False):
    p = {
        "type": "timeseries",
        "title": title,
        "description": description,
        "datasource": DS,
        "targets": [target(e, l, chr(ord("A") + i)) for i, (e, l) in enumerate(targets)],
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                **({} if signed else {"min": 0}),
                "custom": {
                    "lineWidth": 2,
                    "fillOpacity": 12 if stack else 6,
                    "showPoints": "never",
                    "spanNulls": True,
                    "stacking": {"mode": "normal" if stack else "none", "group": "A"},
                },
            },
            "overrides": [],
        },
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "desc"},
        },
    }
    place(p, x, y, w, h)


def q(quantile, metric, by=""):
    group = f"le{', ' + by if by else ''}"
    return f"histogram_quantile({quantile}, sum by ({group}) (rate({metric}_bucket[1m])))"


# Row 1: the cluster at a glance
stat("Data nodes up", 'sum(up{job="data"})', "none", 0, 4, color="green")
# Which PD node leads now (an instant query: a range would show every node that led in the window).
stat(
    "PD leader",
    "max by (node) (distdb_pd_leader) == 1",
    "none", 4, 4, legend="{{node}}", color="purple", instant=True,
)
stat("Ranges", "max(distdb_pd_ranges)", "none", 8, 4, color="purple")
stat("SQL statements / s", "sum(rate(distdb_sql_statements_total[1m]))", "ops", 12, 4, decimals=1)
stat("SQL p99", q(0.99, "distdb_sql_statement_latency_seconds"), "s", 16, 4, color="orange")
stat("Txn conflicts / s", "sum(rate(distdb_txn_conflicts_total[1m]))", "ops", 20, 4, decimals=2, color="red")

# SQL
series(
    "SQL statements per second",
    [
        ("sum by (node) (rate(distdb_sql_statements_total[1m]))", "{{node}}"),
        ("sum(rate(distdb_sql_failures_total[1m]))", "failures (all gateways)"),
    ],
    "ops", 0, 4,
    description="Statements each SQL gateway ran, and how many failed.",
)
series(
    "SQL statement latency",
    [
        (q(0.5, "distdb_sql_statement_latency_seconds"), "p50"),
        (q(0.99, "distdb_sql_statement_latency_seconds"), "p99"),
    ],
    "s", 12, 4,
    description="From the statement's arrival at a gateway to its result, all gateways.",
)

# Raft and replication
series(
    "Commit latency p99, by node",
    [(q(0.99, "distdb_commit_latency_seconds", "node"), "{{node}}")],
    "s", 0, 12,
    description="From a write's Raft proposal to its applied answer, on the ranges each node leads.",
)
series(
    "Range leaders, by node",
    [("distdb_leaders", "{{node}}")],
    "none", 12, 12, stack=True,
    description="How many ranges each node leads. Kill a node and watch its leaders move.",
)
series(
    "Raft elections and leadership changes",
    [
        ("sum by (node) (increase(distdb_raft_elections_total[1m]))", "elections {{node}}"),
        ("sum(increase(distdb_raft_leaderships_total[1m]))", "new leaders (all)"),
    ],
    "none", 0, 20,
    description="Per minute. Elections follow a leader's crash, pause or partition.",
)
series(
    "Raft proposals per second, by node",
    [("rate(distdb_raft_proposals_total[1m])", "{{node}}")],
    "ops", 12, 20,
)

# Storage
series(
    "WAL fsyncs per second, by node",
    [("rate(distdb_engine_wal_syncs_total[1m])", "{{node}}")],
    "ops", 0, 28,
    description="Each fsync costs about 2 ms on NTFS; in Docker, whatever the VM's disk costs.",
)
series(
    "Writes per fsync (group commit), by node",
    [
        (
            "rate(distdb_engine_grouped_writes_total[1m]) / rate(distdb_engine_group_commits_total[1m])",
            "{{node}}",
        )
    ],
    "none", 12, 28,
    description="How many concurrent writes shared one fsync.",
)

# Transactions and the oracle
series(
    "Transactions per second",
    [
        ("sum(rate(distdb_txn_committed_total[1m]))", "committed"),
        ("sum(rate(distdb_txn_conflicts_total[1m]))", "conflicts"),
        ("sum(rate(distdb_txn_lock_resolutions_total[1m]))", "locks resolved"),
        ("sum(rate(distdb_sql_autocommit_retries_total[1m]))", "autocommit retries"),
    ],
    "ops", 0, 36,
)
series(
    "Timestamp oracle",
    [
        ("sum(rate(distdb_pd_tso_timestamps_total[1m]))", "timestamps / s"),
        ("sum(rate(distdb_pd_tso_requests_total[1m]))", "requests / s"),
    ],
    "ops", 12, 36,
    description="Fewer requests than timestamps: clients batch their timestamp requests.",
)

# Chaos and housekeeping
series(
    "Clock skew injected, by node",
    [("distdb_clock_skew_milliseconds", "{{node}}")],
    "ms", 0, 44,
    description="Set by the chaos suite through /debug/clock.",
    signed=True,
)
series(
    "Compaction, bytes written per second",
    [("sum by (node) (rate(distdb_engine_compaction_written_bytes_total[1m]))", "{{node}}")],
    "Bps", 12, 44,
)

dashboard = {
    "uid": "distdb",
    "title": "DistDB",
    "tags": ["distdb"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "panels": panels,
}
out = pathlib.Path(__file__).parent / "grafana" / "dashboards" / "distdb.json"
out.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8", newline="\n")
print(f"wrote {out} ({len(panels)} panels)")
