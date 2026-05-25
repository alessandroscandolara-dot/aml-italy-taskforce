"""
AML Italy — Task Force Giornaliera
===================================
Esegue ogni mattina (lun-ven) alle 08:00:
1. Interroga Snowflake per le 7 task force di allerte Awaiting
2. Crea una nuova riga nel database "Task Force date" in Notion
3. Crea il database inline delle allerte dentro quella riga
4. Popola il database con tutte le allerte trovate (batch da 100)
5. Invia notifica su Slack #aml-italy con il link diretto

Dipendenze: snowflake-connector-python, requests
Secrets GitHub richiesti:
  SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_TOKEN,
  SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_ROLE,
  NOTION_TOKEN, SLACK_TOKEN, SLACK_CHANNEL_ID,
  NOTION_TF_DATE_DB, NOTION_TF_DATE_DS
"""

import os
import sys
import json
import math
import requests
import snowflake.connector
from datetime import date, datetime

# ─────────────────────────────────────────────
# CONFIG DA ENVIRONMENT
# ─────────────────────────────────────────────
SF_ACCOUNT   = os.environ["SNOWFLAKE_ACCOUNT"]
SF_USER      = os.environ["SNOWFLAKE_USER"]
SF_TOKEN     = os.environ["SNOWFLAKE_TOKEN"]
SF_WH        = os.environ["SNOWFLAKE_WAREHOUSE"]
SF_DB        = os.environ["SNOWFLAKE_DATABASE"]
SF_ROLE      = os.environ["SNOWFLAKE_ROLE"]

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
NOTION_TF_DATE_DB = os.environ["NOTION_TF_DATE_DB"]   # ID database "Task Force date"
NOTION_TF_DATE_DS = os.environ["NOTION_TF_DATE_DS"]   # Data source ID

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")  # opzionale — aggiungere quando arriva approvazione IT

TODAY     = date.today()
TODAY_ISO = TODAY.isoformat()
TODAY_IT  = TODAY.strftime("%d/%m/%Y")

# ─────────────────────────────────────────────
# SNOWFLAKE
# ─────────────────────────────────────────────
def get_sf_conn():
    return snowflake.connector.connect(
        account=SF_ACCOUNT,
        user=SF_USER,
        token=SF_TOKEN,
        authenticator="oauth",
        warehouse=SF_WH,
        database=SF_DB,
        role=SF_ROLE,
        schema="PUBLIC",
    )

SQL_TASK_FORCE = """
WITH base AS (
    SELECT alert_id, organization_id, time_to_treat, compliance_type,
           account_closing_requested_at, alerts
    FROM ANALYTICS.dashboards.aml_dashboard_aml_alerts
    WHERE legal_country = 'IT' AND team = 'aml'
      AND status = 0 AND is_to_treat = TRUE
      AND priority_score IN (401,402,403,404,501,502,503,504,505,506,507,602)
),
orgs AS (
    SELECT organization_id, status AS org_status, deactivated_at
    FROM staging.api.organizations
),
income_totals AS (
    SELECT ba.organization_id, SUM(i.income_amount_euros) AS total_income
    FROM staging.api.incomes i
    JOIN staging.api.bank_accounts ba ON ba.bank_account_id = i.bank_account_id
    GROUP BY 1
),
has_tx AS (
    SELECT DISTINCT ba.organization_id
    FROM staging.api.transactions t
    JOIN staging.api.bank_accounts ba ON ba.bank_account_id = t.bank_account_id
    WHERE t.api_deleted_at IS NULL AND t.status = 1
),
under_watch AS (
    SELECT DISTINCT e.alert_id
    FROM ANALYTICS.dashboards.aml_dashboard_aml_alerts e
    WHERE e.legal_country = 'IT' AND e.team = 'aml'
      AND e.status = 0 AND e.is_to_treat = TRUE
      AND e.priority_score IN (401,402,403,404,501,502,503,504,505,506,507,602)
      AND EXISTS (
          SELECT 1 FROM ANALYTICS.dashboards.aml_dashboard_aml_alerts e2
          WHERE e2.organization_id = e.organization_id
            AND e2.alert_id != e.alert_id
            AND e2.status IN (2,3,4,5,6,8,9,10,11)
            AND UPPER(e2.risk_type_at_closure) IN ('STR','UNDER_WATCH','MONEY_LAUNDERING')
            AND e2.is_true_positive = TRUE
      )
)
SELECT
    b.alert_id,
    b.organization_id,
    CAST(b.time_to_treat AS DATE)  AS to_treat_date,
    b.compliance_type,
    ROUND(it.total_income, 0)      AS income_euros,
    CASE
        WHEN uw.alert_id IS NOT NULL
             THEN 'TF1 — Under Watch'
        WHEN b.compliance_type = 2 AND o.org_status = 3
             AND o.deactivated_at <= DATEADD('month',-12,CURRENT_DATE())
             AND ht.organization_id IS NOT NULL
             THEN 'TF2 — Legal Request >12m + TX'
        WHEN b.compliance_type = 2 AND o.org_status = 3
             AND ht.organization_id IS NULL
             THEN 'TF3 — Legal Request no TX'
        WHEN b.compliance_type = 15
             THEN 'TF4 — Criminal Seizure'
        WHEN b.compliance_type = 12
             THEN 'TF5 — Adverse Media'
        WHEN it.total_income IS NOT NULL AND it.total_income <= 50000
             THEN 'TF6 — Income ≤50K'
        WHEN it.total_income > 50000 AND it.total_income <= 100000
             THEN 'TF7 — Income 50K–100K'
        ELSE NULL
    END AS task_force
FROM base b
LEFT JOIN orgs o        ON o.organization_id = b.organization_id
LEFT JOIN income_totals it ON it.organization_id = b.organization_id
LEFT JOIN has_tx ht     ON ht.organization_id = b.organization_id
LEFT JOIN under_watch uw ON uw.alert_id = b.alert_id
WHERE
    uw.alert_id IS NOT NULL
    OR (b.compliance_type = 2 AND o.org_status = 3
        AND o.deactivated_at <= DATEADD('month',-12,CURRENT_DATE())
        AND ht.organization_id IS NOT NULL)
    OR (b.compliance_type = 2 AND o.org_status = 3
        AND ht.organization_id IS NULL)
    OR b.compliance_type = 15
    OR b.compliance_type = 12
    OR (it.total_income IS NOT NULL AND it.total_income <= 50000)
    OR (it.total_income > 50000 AND it.total_income <= 100000)
ORDER BY task_force, to_treat_date ASC
"""

def fetch_alerts():
    print(f"[Snowflake] Connessione in corso...")
    conn = get_sf_conn()
    cur  = conn.cursor()
    cur.execute(SQL_TASK_FORCE)
    cols = [c[0].lower() for c in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    print(f"[Snowflake] {len(rows)} allerte trovate")
    return rows

# ─────────────────────────────────────────────
# NOTION HELPERS
# ─────────────────────────────────────────────
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

def notion_post(endpoint, payload):
    r = requests.post(
        f"https://api.notion.com/v1/{endpoint}",
        headers=NOTION_HEADERS,
        json=payload,
        timeout=30,
    )
    if r.status_code not in (200, 201):
        print(f"[Notion] ERRORE {r.status_code}: {r.text[:400]}")
        r.raise_for_status()
    return r.json()

def notion_patch(endpoint, payload):
    r = requests.patch(
        f"https://api.notion.com/v1/{endpoint}",
        headers=NOTION_HEADERS,
        json=payload,
        timeout=30,
    )
    if r.status_code not in (200, 201):
        print(f"[Notion] ERRORE PATCH {r.status_code}: {r.text[:400]}")
        r.raise_for_status()
    return r.json()

# ─────────────────────────────────────────────
# STEP 1 — Crea riga in "Task Force date"
# ─────────────────────────────────────────────
def create_tf_date_row(n_alerts: int) -> str:
    """Crea la riga del giorno nel database Task Force date.
    Ritorna il page_id della riga appena creata."""
    payload = {
        "parent": {"database_id": NOTION_TF_DATE_DB},
        "properties": {
            "Name": {
                "title": [{"text": {"content": f"Task Force {TODAY_IT}"}}]
            },
            "Task force date": {
                "date": {"start": TODAY_ISO}
            },
        },
    }
    result = notion_post("pages", payload)
    page_id = result["id"]
    print(f"[Notion] Riga 'Task Force {TODAY_IT}' creata: {page_id}")
    return page_id

# ─────────────────────────────────────────────
# STEP 2 — Crea database inline nella riga
# ─────────────────────────────────────────────
def create_alerts_db(parent_page_id: str) -> str:
    """Crea il database inline delle allerte dentro la riga del giorno.
    Ritorna il database_id del database appena creato."""
    payload = {
        "parent": {"page_id": parent_page_id, "type": "page_id"},
        "title": [{"text": {"content": f"🚨 Task Force Allerte — {TODAY_IT}"}}],
        "is_inline": True,
        "properties": {
            "Alert ID": {"title": {}},
            "Org ID": {"rich_text": {}},
            "Task Force": {
                "select": {
                    "options": [
                        {"name": "TF1 — Under Watch",           "color": "red"},
                        {"name": "TF2 — Legal Request >12m + TX","color": "green"},
                        {"name": "TF3 — Legal Request no TX",   "color": "green"},
                        {"name": "TF4 — Criminal Seizure",      "color": "purple"},
                        {"name": "TF5 — Adverse Media",         "color": "blue"},
                        {"name": "TF6 — Income ≤50K",           "color": "orange"},
                        {"name": "TF7 — Income 50K–100K",       "color": "yellow"},
                    ]
                }
            },
            "To Treat At": {"date": {}},
            "Income €": {"number": {"format": "euro"}},
            "Livello": {
                "select": {
                    "options": [
                        {"name": "Junior", "color": "blue"},
                        {"name": "Senior", "color": "orange"},
                    ]
                }
            },
            "Agente": {"people": {}},
            "Status": {
                "select": {
                    "options": [
                        {"name": "Not started", "color": "gray"},
                        {"name": "In progress", "color": "yellow"},
                        {"name": "Done",        "color": "green"},
                    ]
                }
            },
        },
    }
    result = notion_post("databases", payload)
    db_id = result["id"]
    print(f"[Notion] Database allerte creato: {db_id}")
    return db_id

# ─────────────────────────────────────────────
# STEP 3 — Popola database allerte (batch 100)
# ─────────────────────────────────────────────
def build_page_payload(row: dict) -> dict:
    """Costruisce il payload per una singola allerta."""
    props = {
        "Alert ID": {
            "title": [{"text": {"content": row["alert_id"] or ""}}]
        },
        "Org ID": {
            "rich_text": [{"text": {"content": row["organization_id"] or ""}}]
        },
        "Status": {"select": {"name": "Not started"}},
    }
    SENIOR_TFS = {"TF6 — Income ≤50K", "TF7 — Income 50K–100K"}
    if row.get("task_force"):
        tf = row["task_force"]
        props["Task Force"] = {"select": {"name": tf}}
        props["Livello"] = {"select": {"name": "Senior" if tf in SENIOR_TFS else "Junior"}}
    if row.get("to_treat_date"):
        d = row["to_treat_date"]
        date_str = d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]
        props["To Treat At"] = {"date": {"start": date_str}}
    income = row.get("income_euros")
    if income is not None:
        try:
            props["Income €"] = {"number": float(income)}
        except (TypeError, ValueError):
            pass
    return props

def populate_alerts_db(db_id: str, alerts: list):
    """Inserisce le allerte nel database a batch di 100."""
    total   = len(alerts)
    batches = math.ceil(total / 100)
    print(f"[Notion] Inserimento {total} allerte in {batches} batch...")

    for i in range(batches):
        batch = alerts[i * 100 : (i + 1) * 100]
        for row in batch:
            props = build_page_payload(row)
            notion_post("pages", {
                "parent": {"database_id": db_id},
                "properties": props,
            })
        print(f"[Notion] Batch {i+1}/{batches} completato ({len(batch)} righe)")

# ─────────────────────────────────────────────
# STEP 4 — Notifica Slack
# ─────────────────────────────────────────────
TF_EMOJI = {
    "TF1 — Under Watch":            "🔴",
    "TF2 — Legal Request >12m + TX":"🟢",
    "TF3 — Legal Request no TX":    "🟢",
    "TF4 — Criminal Seizure":       "🟣",
    "TF5 — Adverse Media":          "🔵",
    "TF6 — Income ≤50K":            "🟠",
    "TF7 — Income 50K–100K":        "🟡",
}

def send_slack(page_url: str, alerts: list):
    """Invia notifica Slack via webhook. Se SLACK_WEBHOOK_URL non è configurato, salta."""
    if not SLACK_WEBHOOK_URL:
        print("[Slack] SLACK_WEBHOOK_URL non configurato — notifica saltata")
        return

    counts = {}
    for row in alerts:
        tf = row.get("task_force") or "Altro"
        counts[tf] = counts.get(tf, 0) + 1

    tf_lines = []
    for tf, emoji in TF_EMOJI.items():
        n = counts.get(tf, 0)
        if n > 0:
            senior = " ⭐ Solo Senior" if tf in ("TF6 — Income ≤50K", "TF7 — Income 50K–100K") else ""
            tf_lines.append(f"{emoji} *{tf}*{senior}: {n} allerte")

    lines_text = "\n".join(tf_lines) if tf_lines else "Nessuna allerta trovata oggi."
    total = len(alerts)

    text = (
        f":rotating_light: *Task Force AML Italia — {TODAY_IT}* :it:\n\n"
        f"Il database delle allerte Awaiting è aggiornato per oggi. "
        f"*{total} allerte totali* su {len(counts)} task force:\n\n"
        f"{lines_text}\n\n"
        f":clipboard: *Database Notion:* {page_url}\n\n"
        f"Compilate il campo *Agente* con il vostro nome e aggiornate lo *Status* man mano che lavorate."
    )

    r = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=15)
    if r.status_code == 200:
        print("[Slack] Messaggio inviato")
    else:
        print(f"[Slack] ERRORE {r.status_code}: {r.text[:200]}")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print(f"\n{'='*50}")
    print(f"AML Italy Task Force — {TODAY_IT}")
    print(f"{'='*50}\n")

    # 1. Fetch allerte da Snowflake
    alerts = fetch_alerts()
    if not alerts:
        print("[INFO] Nessuna allerta trovata oggi. Script terminato.")
        sys.exit(0)

    # 2. Crea riga nel database "Task Force date"
    tf_page_id = create_tf_date_row(len(alerts))

    # 3. Crea database inline delle allerte
    alerts_db_id = create_alerts_db(tf_page_id)

    # 4. Popola il database
    populate_alerts_db(alerts_db_id, alerts)

    # URL della pagina del giorno (per Slack)
    page_url = f"https://www.notion.so/{tf_page_id.replace('-', '')}"

    # 5. Notifica Slack
    send_slack(page_url, alerts)

    # Riepilogo finale
    counts = {}
    for row in alerts:
        tf = row.get("task_force") or "Altro"
        counts[tf] = counts.get(tf, 0) + 1

    print(f"\n{'='*50}")
    print(f"✅ Completato — {len(alerts)} allerte inserite")
    for tf, n in sorted(counts.items()):
        print(f"   {tf}: {n}")
    print(f"   Notion: {page_url}")
    print(f"{'='*50}\n")

if __name__ == "__main__":
    main()
