"""
Lead Generation Pipeline — Dashboard
Run with: .venv/bin/streamlit run dashboard.py
"""

import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

DB_PATH = Path("output/leads.db")

st.set_page_config(page_title="Lead Pipeline", page_icon="🔍", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .stButton > button { width: 100%; }
</style>
""", unsafe_allow_html=True)


def get_conn():
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ═══════════════════════════════════════════════════════════════
# TABS
# ═══════════════════════════════════════════════════════════════

tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "👥 Contacts", "🏢 Companies", "🧹 Verify & Clean"])


# ───────────────────────────────────────────────────────────────
# TAB 1 — OVERVIEW
# ───────────────────────────────────────────────────────────────

with tab1:
    st.subheader("Pipeline Overview")

    col_ref, _ = st.columns([1, 5])
    with col_ref:
        if st.button("↻ Refresh"):
            st.rerun()

    conn = get_conn()
    if not conn:
        st.error("Database not found. Run --step discover first.")
        st.stop()

    # KPIs
    total_co  = conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
    pending   = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=0").fetchone()[0]
    scraped   = conn.execute("SELECT COUNT(*) FROM companies WHERE scraped=1").fetchone()[0]
    total_ct  = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()[0]
    confirmed = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='confirmed'").fetchone()[0]
    deliv     = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='deliverable'").fetchone()[0]
    pct       = round(scraped / total_co * 100, 1) if total_co else 0

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Companies",   f"{total_co:,}")
    k2.metric("Pending Scrape",    f"{pending:,}")
    k3.metric("Scraped",           f"{scraped:,}", f"{pct}%")
    k4.metric("Total Contacts",    f"{total_ct:,}")
    k5.metric("Confirmed",         f"{confirmed:,}")
    k6.metric("Deliverable Emails",f"{deliv:,}")

    st.progress(pct / 100, text=f"Scraping progress: {scraped:,} / {total_co:,} ({pct}%)")
    st.divider()

    # Charts
    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Companies by Country** (top 20 with known country)")
        rows = conn.execute("""
            SELECT country, COUNT(*) as n FROM companies
            WHERE country != '' AND country IS NOT NULL
            GROUP BY country ORDER BY n DESC LIMIT 20
        """).fetchall()
        if rows:
            df = pd.DataFrame(rows, columns=["Country", "Count"])
            fig = px.bar(df, x="Count", y="Country", orientation="h",
                         color="Count", color_continuous_scale="Blues", height=400)
            fig.update_layout(coloraxis_showscale=False, margin=dict(l=0,r=0,t=5,b=0),
                              yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown("**Contacts by Status**")
        rows2 = conn.execute("""
            SELECT email_status, COUNT(*) as n FROM contacts
            GROUP BY email_status ORDER BY n DESC
        """).fetchall()
        if rows2:
            df2 = pd.DataFrame(rows2, columns=["Status", "Count"])
            fig2 = px.pie(df2, names="Status", values="Count",
                          color_discrete_sequence=px.colors.qualitative.Set2,
                          hole=0.5, height=350)
            fig2.update_layout(margin=dict(l=0,r=0,t=5,b=0))
            st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # Pipeline controls
    st.subheader("Run Pipeline")

    p1, p2, p3, p4 = st.columns(4)
    with p1:
        if st.button("🔎 Discover Companies"):
            st.session_state["cmd"] = [sys.executable, "run_pipeline.py", "--step", "discover"]
    with p2:
        limit = st.number_input("Limit", 10, 100000, 1000, 500, label_visibility="collapsed")
        if st.button(f"🕷 Scrape {limit:,}"):
            st.session_state["cmd"] = [sys.executable, "run_pipeline.py", "--step", "scrape", "--limit", str(limit)]
    with p3:
        if st.button("👥 Find People"):
            st.session_state["cmd"] = [sys.executable, "run_pipeline.py", "--step", "people"]
    with p4:
        if st.button("📤 Export CSV"):
            st.session_state["cmd"] = [sys.executable, "run_pipeline.py", "--export"]

    if "cmd" in st.session_state:
        cmd = st.session_state.pop("cmd")
        st.markdown(f"`{' '.join(cmd[1:])}`")
        box   = st.empty()
        lines = []
        proc  = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            lines.append(line.rstrip())
            box.code("\n".join(lines[-40:]))
        proc.wait()
        st.success("Done!" if proc.returncode == 0 else f"Exit code {proc.returncode}")
        st.rerun()

    conn.close()


# ───────────────────────────────────────────────────────────────
# TAB 2 — CONTACTS BROWSER
# ───────────────────────────────────────────────────────────────

with tab2:
    st.subheader("Contacts Browser")

    conn = get_conn()

    # Filters row
    f1, f2, f3, f4 = st.columns([2, 2, 2, 2])
    with f1:
        search = st.text_input("Search name / email / company", placeholder="e.g. john, stripe.com")
    with f2:
        statuses = ["All", "confirmed", "deliverable", "unverified", "no_email", "undeliverable"]
        status_filter = st.selectbox("Email Status", statuses)
    with f3:
        sources = ["All"] + [r[0] for r in conn.execute(
            "SELECT DISTINCT source FROM contacts WHERE source != '' ORDER BY source"
        ).fetchall()]
        source_filter = st.selectbox("Source", sources)
    with f4:
        page_size = st.selectbox("Rows per page", [50, 100, 200, 500], index=1)

    # Build query
    where  = []
    params = []

    if search:
        where.append("(ct.first_name || ' ' || ct.last_name LIKE ? OR ct.email LIKE ? OR ct.company LIKE ? OR ct.company_domain LIKE ?)")
        s = f"%{search}%"
        params += [s, s, s, s]

    if status_filter != "All":
        where.append("ct.email_status = ?")
        params.append(status_filter)

    if source_filter != "All":
        where.append("ct.source = ?")
        params.append(source_filter)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    # Count
    count = conn.execute(f"""
        SELECT COUNT(*) FROM contacts ct
        LEFT JOIN companies co ON ct.company_id = co.id
        {where_sql}
    """, params).fetchone()[0]

    st.caption(f"{count:,} contacts match")

    # Pagination
    total_pages = max(1, (count + page_size - 1) // page_size)
    page = st.number_input("Page", 1, total_pages, 1, label_visibility="collapsed")
    offset = (page - 1) * page_size

    rows = conn.execute(f"""
        SELECT
            ct.first_name || ' ' || ct.last_name  AS Name,
            ct.title                               AS Title,
            ct.company                             AS Company,
            ct.company_domain                      AS Domain,
            co.country                             AS Country,
            ct.email                               AS Email,
            ct.email_status                        AS Status,
            ct.source                              AS Source,
            substr(ct.created_at, 1, 10)           AS Added
        FROM contacts ct
        LEFT JOIN companies co ON ct.company_id = co.id
        {where_sql}
        ORDER BY ct.created_at DESC
        LIMIT ? OFFSET ?
    """, params + [page_size, offset]).fetchall()

    if rows:
        STATUS_ICON = {
            "confirmed":    "✅",
            "deliverable":  "🟢",
            "unverified":   "🟡",
            "undeliverable":"🔴",
            "no_email":     "⚪",
        }
        df = pd.DataFrame(rows, columns=["Name","Title","Company","Domain","Country","Email","Status","Source","Added"])
        df["Status"] = df["Status"].map(lambda s: f"{STATUS_ICON.get(s,'❓')} {s}")
        st.dataframe(df, use_container_width=True, hide_index=True,
                     column_config={
                         "Email":  st.column_config.TextColumn(width="medium"),
                         "Title":  st.column_config.TextColumn(width="medium"),
                         "Name":   st.column_config.TextColumn(width="medium"),
                     })
        st.caption(f"Page {page} of {total_pages}")
    else:
        st.info("No contacts found.")

    conn.close()


# ───────────────────────────────────────────────────────────────
# TAB 3 — COMPANIES BROWSER
# ───────────────────────────────────────────────────────────────

with tab3:
    st.subheader("Companies Browser")

    conn = get_conn()

    # Filters
    g1, g2, g3 = st.columns([3, 2, 2])
    with g1:
        co_search = st.text_input("Search name / domain", placeholder="e.g. stripe, .io")
    with g2:
        co_status = st.selectbox("Scrape Status", ["All", "Pending", "Scraped"])
    with g3:
        co_page_size = st.selectbox("Rows", [50, 100, 200], index=1, key="co_ps")

    # Countries for filter
    country_options = ["All"] + [r[0] for r in conn.execute(
        "SELECT DISTINCT country FROM companies WHERE country != '' ORDER BY country"
    ).fetchall()]
    co_country = st.selectbox("Country", country_options)

    where2  = []
    params2 = []

    if co_search:
        where2.append("(name LIKE ? OR domain LIKE ?)")
        s = f"%{co_search}%"
        params2 += [s, s]
    if co_status == "Pending":
        where2.append("scraped = 0")
    elif co_status == "Scraped":
        where2.append("scraped = 1")
    if co_country != "All":
        where2.append("country = ?")
        params2.append(co_country)

    where2_sql = ("WHERE " + " AND ".join(where2)) if where2 else ""

    co_count = conn.execute(f"SELECT COUNT(*) FROM companies {where2_sql}", params2).fetchone()[0]
    st.caption(f"{co_count:,} companies match")

    co_total_pages = max(1, (co_count + co_page_size - 1) // co_page_size)
    co_page = st.number_input("Page", 1, co_total_pages, 1, label_visibility="collapsed", key="co_page")
    co_offset = (co_page - 1) * co_page_size

    co_rows = conn.execute(f"""
        SELECT name, domain, country, source,
               CASE WHEN scraped=1 THEN '✅ Scraped' ELSE '⏳ Pending' END as status,
               substr(added_at, 1, 10) as added
        FROM companies
        {where2_sql}
        ORDER BY added_at DESC
        LIMIT ? OFFSET ?
    """, params2 + [co_page_size, co_offset]).fetchall()

    if co_rows:
        df_co = pd.DataFrame(co_rows, columns=["Name","Domain","Country","Source","Status","Added"])
        st.dataframe(df_co, use_container_width=True, hide_index=True)
        st.caption(f"Page {co_page} of {co_total_pages}")
    else:
        st.info("No companies found.")

    conn.close()


# ───────────────────────────────────────────────────────────────
# TAB 4 — VERIFY & CLEAN
# ───────────────────────────────────────────────────────────────

with tab4:
    st.subheader("Verify & Clean Database")

    conn = get_conn()

    # Current status breakdown
    st.markdown("**Current Email Status Breakdown**")
    status_rows = conn.execute("""
        SELECT email_status, COUNT(*) as n FROM contacts
        GROUP BY email_status ORDER BY n DESC
    """).fetchall()

    cols = st.columns(len(status_rows))
    STATUS_COLOR = {
        "deliverable":   "🟢",
        "unknown":       "🔵",
        "confirmed":     "✅",
        "unverified":    "🟡",
        "undeliverable": "🔴",
        "risky":         "🟠",
        "no_email":      "⚪",
    }
    for i, (status, count) in enumerate(status_rows):
        icon = STATUS_COLOR.get(status, "❓")
        cols[i].metric(f"{icon} {status}", f"{count:,}")

    conn.close()

    st.divider()

    # Verification
    st.markdown("**Run Email Verification**")
    st.caption("Checks each email via SMTP — confirms it can actually receive mail.")

    v1, v2 = st.columns(2)
    with v1:
        smtp_on = st.toggle("SMTP check (deep, slower)", value=True)
    with v2:
        workers = st.slider("Parallel workers", 5, 50, 30)

    if st.button("▶ Run Verification", type="primary"):
        cmd = [sys.executable, "run_pipeline.py", "--verify-db"]
        if smtp_on:
            cmd.append("--smtp")
        st.session_state["verify_cmd"] = cmd

    if "verify_cmd" in st.session_state:
        cmd = st.session_state.pop("verify_cmd")
        st.markdown(f"`{' '.join(cmd[1:])}`")
        box   = st.empty()
        lines = []
        proc  = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        for line in proc.stdout:
            lines.append(line.rstrip())
            box.code("\n".join(lines[-30:]))
        proc.wait()
        st.success("Verification complete!" if proc.returncode == 0 else f"Exit code {proc.returncode}")
        st.rerun()

    st.divider()

    # Cleanup
    st.markdown("**Remove Bad Contacts**")

    conn = get_conn()
    undeliv = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='undeliverable'").fetchone()[0]
    risky   = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='risky'").fetchone()[0]
    noemail = conn.execute("SELECT COUNT(*) FROM contacts WHERE email_status='no_email' OR email LIKE '_noemail_%'").fetchone()[0]
    conn.close()

    d1, d2, d3 = st.columns(3)
    d1.metric("🔴 Undeliverable", f"{undeliv:,}")
    d2.metric("🟠 Risky",         f"{risky:,}")
    d3.metric("⚪ No Email",      f"{noemail:,}")

    st.warning("This permanently deletes contacts from the database. Cannot be undone.")

    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button(f"🗑 Delete Undeliverable ({undeliv:,})", disabled=undeliv == 0):
            conn = get_conn()
            n = conn.execute("DELETE FROM contacts WHERE email_status='undeliverable'").rowcount
            conn.commit(); conn.close()
            st.success(f"Deleted {n:,} undeliverable contacts.")
            st.rerun()
    with c2:
        if st.button(f"🗑 Delete Risky ({risky:,})", disabled=risky == 0):
            conn = get_conn()
            n = conn.execute("DELETE FROM contacts WHERE email_status='risky'").rowcount
            conn.commit(); conn.close()
            st.success(f"Deleted {n:,} risky contacts.")
            st.rerun()
    with c3:
        if st.button(f"🗑 Delete No-Email ({noemail:,})", disabled=noemail == 0):
            conn = get_conn()
            n = conn.execute("DELETE FROM contacts WHERE email_status='no_email' OR email LIKE '_noemail_%'").rowcount
            conn.commit(); conn.close()
            st.success(f"Deleted {n:,} no-email contacts.")
            st.rerun()

    st.markdown("&nbsp;")
    if st.button("🗑 Delete ALL bad contacts (undeliverable + risky + no-email)", type="secondary"):
        conn = get_conn()
        n = conn.execute("""
            DELETE FROM contacts
            WHERE email_status IN ('undeliverable', 'risky', 'no_email')
               OR email LIKE '_noemail_%'
        """).rowcount
        conn.commit(); conn.close()
        st.success(f"Deleted {n:,} bad contacts.")
        st.rerun()
