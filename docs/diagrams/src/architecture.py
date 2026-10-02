"""Generates docs/diagrams/architecture.svg.

Hand-laid-out system architecture diagram. Run:  python3 docs/diagrams/src/architecture.py
Coordinates are explicit on purpose: the diagram is a designed artefact, not auto-layout.
"""
from pathlib import Path
from xml.sax.saxutils import escape

W, H = 1680, 1025
FONT = "-apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif"
MONO = "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace"

INK, MUTED, EDGE, HOT = "#0F172A", "#475569", "#64748B", "#7C3AED"

LANES = {
    "sources":   ("#475569", "#F8FAFC"),
    "ingest":    ("#4338CA", "#EEF2FF"),
    "storage":   ("#0F766E", "#F0FDFA"),
    "cdc":       ("#C2410C", "#FFF7ED"),
    "index":     ("#7C3AED", "#F5F3FF"),
    "serving":   ("#1D4ED8", "#EFF6FF"),
}
TAGS = {
    "REAL":      ("#166534", "#DCFCE7"),
    "SIMULATED": ("#92400E", "#FEF3C7"),
    "OPTIONAL":  ("#475569", "#F1F5F9"),
    "LLM":       ("#1E40AF", "#DBEAFE"),
}

out: list[str] = []


def text(x, y, s, size=13, weight=400, fill=INK, anchor="start", family=FONT, extra=""):
    out.append(
        f'<text x="{x}" y="{y}" font-family="{family}" font-size="{size}" font-weight="{weight}" '
        f'fill="{fill}" text-anchor="{anchor}" {extra}>{escape(s)}</text>'
    )


def lane(x, y, w, h, title, key, num):
    stroke, fill = LANES[key]
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="14" fill="{fill}" stroke="{stroke}" stroke-opacity="0.35"/>')
    out.append(f'<circle cx="{x + 22}" cy="{y + 24}" r="11" fill="{stroke}"/>')
    text(x + 22, y + 28.5, str(num), 12, 700, "#FFFFFF", "middle")
    text(x + 40, y + 29, title.upper(), 12, 700, stroke, extra='letter-spacing="0.08em"')


def tag(x, y, label):
    fg, bg = TAGS[label]
    w = 8 + len(label) * 6.6
    out.append(f'<rect x="{x}" y="{y}" width="{w:.0f}" height="18" rx="9" fill="{bg}"/>')
    text(x + w / 2, y + 12.5, label, 9.5, 700, fg, "middle", extra='letter-spacing="0.06em"')
    return w


def node(x, y, w, h, title, lines=(), key="sources", tags=(), dashed=False, mono_lines=(), tags_top=False):
    stroke, _ = LANES[key]
    dash = ' stroke-dasharray="6 4"' if dashed else ""
    out.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" fill="#FFFFFF" '
               f'stroke="{stroke}" stroke-width="1.5"{dash} filter="url(#shadow)"/>')
    out.append(f'<rect x="{x}" y="{y + 10}" width="4" height="{h - 20}" rx="2" fill="{stroke}"/>')
    text(x + 16, y + 24, title, 14.5, 650)
    ty = y + 44
    for ln in lines:
        text(x + 16, ty, ln, 12, 400, MUTED)
        ty += 17
    for ln in mono_lines:
        text(x + 16, ty, ln, 11, 500, stroke, family=MONO)
        ty += 16
    if tags_top:
        tx = x + w - 12
        for t in tags:
            tx -= 8 + len(t) * 6.6
            tag(tx, y + 11, t)
            tx -= 6
        return
    tx = x + 16
    for t in tags:
        tx += tag(tx, y + h - 26, t) + 6


def edge(pts, label=None, at=None, hot=False, dashed=False, anchor="middle"):
    color = HOT if hot else EDGE
    width = 2.4 if hot else 1.6
    marker = "arrow-hot" if hot else "arrow"
    d = "M" + " L".join(f"{px},{py}" for px, py in pts)
    dash = ' stroke-dasharray="6 5"' if dashed else ""
    out.append(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}"{dash} '
               f'stroke-linejoin="round" marker-end="url(#{marker})"/>')
    if label:
        lx, ly = at
        text(lx, ly, label, 11, 600 if hot else 500, HOT if hot else MUTED, anchor,
             extra='paint-order="stroke" stroke="#FFFFFF" stroke-width="5" stroke-linejoin="round"')


def step(x, y, n):
    out.append(f'<circle cx="{x}" cy="{y}" r="11" fill="{HOT}" stroke="#FFFFFF" stroke-width="2"/>')
    text(x, y + 4.5, str(n), 11.5, 700, "#FFFFFF", "middle")


# ---------------------------------------------------------------- canvas
out.append(f'<svg xmlns="http://www.w3.org/2000/svg" width="{W}" height="{H}" viewBox="0 0 {W} {H}" '
           f'role="img" aria-labelledby="t d">')
out.append('<title id="t">Freshness Control Plane — system architecture</title>')
out.append('<desc id="d">Aviation sources are ingested into Postgres and Iceberg; Debezium streams Postgres '
           'changes through Kafka to a selective re-index consumer that re-embeds only changed chunks into '
           'pgvector and updates a freshness registry; an SLA enforcer evaluates freshness contracts; a '
           'Claude-based agent checks freshness at query time and labels answers.</desc>')
out.append(f"""<defs>
  <marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse">
    <path d="M0,0 L10,5 L0,10 z" fill="{EDGE}"/></marker>
  <marker id="arrow-hot" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="6" markerHeight="6" orient="auto-start-reverse">
    <path d="M0,0 L10,5 L0,10 z" fill="{HOT}"/></marker>
  <filter id="shadow" x="-10%" y="-10%" width="120%" height="130%">
    <feDropShadow dx="0" dy="1.5" stdDeviation="2" flood-color="#0F172A" flood-opacity="0.10"/></filter>
</defs>""")
out.append(f'<rect width="{W}" height="{H}" rx="18" fill="#FFFFFF"/>')

text(32, 50, "Freshness Control Plane", 26, 750)
text(32, 76, "System architecture · v0.1 · change-driven re-indexing with query-time freshness enforcement", 14, 400, MUTED)

# ---------------------------------------------------------------- data plane lanes
lane(20, 100, 270, 560, "Sources", "sources", 1)
lane(305, 100, 280, 560, "Ingestion · Dagster", "ingest", 2)
lane(600, 100, 330, 560, "Storage", "storage", 3)
lane(945, 100, 265, 560, "Change capture", "cdc", 4)
lane(1225, 100, 435, 560, "Index & freshness", "index", 5)
lane(20, 700, 1640, 215, "Serving, evaluation & observability", "serving", 6)

# Sources
node(40, 150, 230, 90, "OpenSky Network", ["Flight states, arrivals / departures"], "sources", ["REAL"])
node(40, 255, 230, 90, "OurAirports", ["Airport & runway reference"], "sources", ["REAL"])
node(40, 385, 230, 90, "BTS On-Time Performance", ["Scheduled vs actual, delays"], "sources", ["REAL"])
node(40, 490, 230, 90, "BTS DB1B", ["10% ticket sample → fare baseline"], "sources", ["REAL"])
text(155, 612, "free tiers only · no scraping", 11.5, 500, MUTED, "middle")
text(155, 630, "OpenSky: OAuth2, ≤80% daily credits", 11.5, 500, MUTED, "middle")

# Ingestion
node(325, 150, 240, 110, "Live pollers", ["OpenSky every 5 min · OAuth2", "OurAirports weekly", "Shared rate limiter + budget"], "ingest")
node(325, 280, 240, 100, "Fare-drift simulator", ["Seasonality × days-to-departure", "+ Poisson price shocks"], "ingest", ["SIMULATED"])
node(325, 400, 240, 100, "Batch loaders", ["BTS On-Time (monthly)", "DB1B (quarterly)"], "ingest")
node(325, 520, 240, 80, "Record / replay", ["Offline demos & CI fixtures"], "ingest", dashed=True)

# Storage
node(620, 150, 270, 180, "Postgres 16 · source.*", ["Current operational state", "Idempotent upsert — unchanged", "rows write nothing to the WAL"],
     "storage", mono_lines=["flight_state · fare · airport", "wal_level = logical"])
node(620, 400, 270, 100, "Iceberg · raw.*", ["Immutable history (PyIceberg)", "Baseline & ground truth"], "storage")
node(620, 535, 270, 90, "dbt marts (dbt-duckdb)", ["Route on-time · fare baseline"], "storage")

# CDC
node(965, 150, 225, 100, "Debezium", ["Postgres connector · pgoutput", "REPLICA IDENTITY FULL"], "cdc")
node(965, 300, 225, 135, "Kafka (KRaft)", ["One topic per source table"], "cdc",
     mono_lines=["fcp.source.flight_state", "fcp.source.fare", "fcp.source.airport", "{op, before, after, ts_ms}"])

# Index & freshness
node(1245, 150, 395, 110, "Selective re-index consumer", ["Re-embeds only chunks whose content hash changed", "Micro-batch ≤64 events / 500 ms · idempotent"],
     "index", mono_lines=["diff → render chunk → sha256 → embed → upsert"])
out.append('<rect x="1240" y="305" width="405" height="150" rx="12" fill="none" stroke="#7C3AED" stroke-opacity="0.55" stroke-dasharray="5 4"/>')
text(1252, 322, "one Postgres txn", 10.5, 600, "#7C3AED")
node(1255, 333, 185, 112, "pgvector", ["index.chunks", "bge-small · 384-d · HNSW"], "index")
node(1450, 333, 185, 112, "Freshness registry", ["freshness.registry", "last_verified_at per record"], "index")
node(1245, 480, 195, 100, "SLA enforcer", ["Dagster sensor · 60 s", "Detects & resolves breaches"], "index")
node(1450, 480, 190, 100, "Freshness contracts", ["Versioned YAML, no deploy", "fare 24 h · status 15 min"], "index",
     mono_lines=["freshness_sla.yaml"])

# Serving
node(45, 755, 170, 120, "Ops / fare analyst", ["Asks ops & fare", "questions in natural", "language"], "serving")
node(290, 745, 340, 150, "Freshness-aware agent", ["Claude · tool use", "Label computed in code, not by the model"], "serving", ["LLM"], tags_top=True,
     mono_lines=["search_index · check_freshness", "get_baseline"])
for i, (lbl, fg, bg) in enumerate([("VERIFIED", "#166534", "#DCFCE7"), ("POSSIBLY STALE", "#92400E", "#FEF3C7"), ("CANNOT ANSWER", "#991B1B", "#FEE2E2")]):
    bx = 306 + [0, 82, 198][i]
    bw = [74, 108, 104][i]
    out.append(f'<rect x="{bx}" y="857" width="{bw}" height="20" rx="4" fill="{bg}" stroke="{fg}" stroke-opacity="0.4"/>')
    text(bx + bw / 2, 871, lbl, 9.5, 700, fg, "middle", extra='letter-spacing="0.04em"')
node(700, 755, 240, 130, "Evaluation harness", ["~50 golden questions", "Naive vs freshness-aware", "Before / during / after drift", "Flag precision & recall"], "serving")
node(1010, 755, 235, 130, "Metrics store", ["Everything the dashboard reads"], "serving",
     mono_lines=["metrics.reindex_log", "metrics.full_rebuild_log", "freshness.sla_breach", "metrics.agent_query_log"])
node(1290, 755, 170, 130, "Dashboard", ["Streamlit", "Freshness lag / source", "Re-index savings", "SLA breach log"], "serving")
node(1480, 755, 160, 130, "Unity Catalog OSS", ["Contracts & SLA", "as table properties"], "serving", ["OPTIONAL"], dashed=True)

# ---------------------------------------------------------------- edges
# sources → ingestion
edge([(270, 195), (325, 195)])
edge([(270, 300), (298, 300), (298, 235), (325, 235)])
edge([(270, 430), (298, 430), (298, 440), (325, 440)])
edge([(270, 535), (298, 535), (298, 470), (325, 470)])
# ingestion → storage
edge([(565, 195), (620, 195)], hot=True)
step(592, 180, 1)
edge([(565, 320), (620, 320)], "upsert", (592, 312))
edge([(565, 450), (620, 450)], "append", (592, 442))
edge([(765, 500), (765, 535)])
edge([(890, 580), (912, 580), (912, 300), (890, 300)])
text(924, 440, "seed fare baseline", 11, 500, MUTED, "middle", extra='transform="rotate(90 924 440)"')
# storage → cdc
edge([(890, 200), (965, 200)], hot=True)
step(940, 185, 2)
text(940, 222, "WAL", 11, 600, HOT, "middle")
edge([(1077, 250), (1077, 300)], hot=True)
step(1095, 275, 3)
# cdc → index
edge([(1190, 368), (1217, 368), (1217, 212), (1245, 212)], hot=True)
step(1217, 300, 4)
edge([(1400, 260), (1400, 333)], hot=True)
edge([(1590, 260), (1590, 333)], hot=True)
step(1400, 292, 5)
step(1590, 292, 5)
edge([(1542, 445), (1542, 465), (1342, 465), (1342, 480)])
edge([(1450, 545), (1440, 545)])
# index → serving
edge([(1342, 580), (1342, 725), (1127, 725), (1127, 755)], "reindex & breach metrics", (1235, 719))
edge([(1545, 580), (1545, 755)], dashed=True)
text(1553, 690, "mirror", 11, 500, MUTED)
edge([(560, 745), (560, 684), (1232, 684), (1232, 389), (1255, 389)], hot=True)
step(820, 684, 6)
text(1000, 678, "search_index + check_freshness at query time", 11, 600, HOT, "middle",
     extra='paint-order="stroke" stroke="#FFFFFF" stroke-width="5"')
edge([(380, 745), (380, 670), (607, 670), (607, 600), (620, 600)], "get_baseline", (470, 686))
# analyst ↔ agent
edge([(215, 800), (290, 800)], "question", (252, 792))
edge([(290, 835), (215, 835)], "answer", (252, 852))
# eval
edge([(700, 810), (630, 810)], "questions", (665, 802))
edge([(940, 828), (1010, 828)], "reports", (975, 820))
edge([(1245, 828), (1290, 828)])

# ---------------------------------------------------------------- legend
LY = 950
text(32, LY + 5, "LEGEND", 11, 700, MUTED, extra='letter-spacing="0.08em"')
out.append(f'<path d="M100,{LY} L140,{LY}" stroke="{HOT}" stroke-width="2.4" marker-end="url(#arrow-hot)"/>')
text(148, LY + 4, "Change hot path (numbered order)", 12, 500, INK)
out.append(f'<path d="M370,{LY} L410,{LY}" stroke="{EDGE}" stroke-width="1.6" marker-end="url(#arrow)"/>')
text(418, LY + 4, "Data / control flow", 12, 500, INK)
out.append(f'<path d="M560,{LY} L600,{LY}" stroke="{EDGE}" stroke-width="1.6" stroke-dasharray="6 5" marker-end="url(#arrow)"/>')
text(608, LY + 4, "Optional", 12, 500, INK)
x = 700
for t, desc in [("REAL", "Real public data"), ("SIMULATED", "Simulated, always labelled"), ("LLM", "Model call"), ("OPTIONAL", "Optional component")]:
    x += tag(x, LY - 9, t) + 8
    text(x, LY + 4, desc, 12, 500, INK)
    x += len(desc) * 6.6 + 26
text(32, LY + 45, "SLO targets: change → index p95 < 60 s · ≥ 90% fewer embeddings than full re-embed · stale-flag recall ≥ 95%",
     12, 500, MUTED)
text(W - 32, LY + 45, "Source: docs/diagrams/src/architecture.py", 11, 400, "#94A3B8", "end")

out.append("</svg>")
dest = Path(__file__).resolve().parents[1] / "architecture.svg"
dest.write_text("\n".join(out), encoding="utf-8")
print(f"wrote {dest}")
