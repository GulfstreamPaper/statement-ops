import os
import sqlite3
from functools import wraps
from datetime import datetime, date, timedelta
from email.message import EmailMessage
from email.utils import make_msgid
import mimetypes
import json
import smtplib
import math
import time
import threading
from io import BytesIO

import pandas as pd
from fpdf import FPDF
from flask import Flask, render_template, request, redirect, url_for, flash, send_file, session, jsonify

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATA_DIR = os.environ.get("DATA_DIR", BASE_DIR)
DB_PATH = os.path.join(DATA_DIR, "statement_app.db")
UPLOAD_DIR = os.path.join(DATA_DIR, "uploads")
OUT_DIR = os.path.join(DATA_DIR, "out")
LOGO_DIR = os.path.join(UPLOAD_DIR, "logos")

REQUIRED_COLUMNS = {"Customer Name", "Order ID", "Order Total", "Shipping Date"}
ALLOWED_IMPORT_EXTENSIONS = {".csv", ".xlsx", ".xls"}
ALLOWED_LOGO_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif"}
RECIPIENT_FREQUENCIES = {"weekly", "biweekly", "monthly", "none"}
TERM_OPTIONS = [
    ("net_7", "Net 7"),
    ("net_15", "Net 15"),
    ("net_20", "Net 20"),
    ("net_30", "Net 30"),
    ("net_45", "Net 45"),
    ("cod", "COD"),
    ("bill_to_bill", "Bill to Bill"),
    ("month_to_month", "Month to Month"),
    ("week_to_week", "Week to Week"),
]
TERM_DAYS = {
    "net_7": 7,
    "net_15": 15,
    "net_20": 20,
    "net_30": 30,
    "net_45": 45,
    "cod": 1,
    "bill_to_bill": 1,
}
TERM_LABEL_TO_CODE = {label.lower(): code for code, label in TERM_OPTIONS}
TERM_CODE_TO_LABEL = {code: label for code, label in TERM_OPTIONS}
EMAIL_TEMPLATE_DEFAULTS = {
    "statement": (
        "Dear Customer,\n\n"
        "Attached please find the most recent statement of open invoices.\n\n"
        "Please let us know if you have any questions.\n\n"
        "Kind regards,\n"
        "Redway Group Inc"
    ),
    "overdue": (
        "Good afternoon Team,\n\n"
        "Attached please find the most recent statement of open invoices. Please update us on the status of payments for all highlighted invoices.\n\n"
        "Let me know if you have any questions or need additional information.\n\n"
        "Kind regards,\n"
        "Redway Group Inc"
    ),
    "skipped": (
        "Dear Customer,\n\n"
        "It seems that one or more invoices have been skipped with your last payment. Attached please find the copies of the invoices along with a most recent statement for the account.\n\n"
        "Please let us know if you have any questions.\n\n"
        "Kind Regards,\n"
        "Redway Group Team"
    ),
    "short_paid": (
        "Dear Customer,\n\n"
        "Attached is a copy of the invoice that appears to have been partially paid. Could you please double check your records. Thanks in advance!\n\n"
        "Kind regards,\n"
        "Redway Group Team"
    ),
}

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-change-me")

APP_USERNAME = os.environ.get("APP_USERNAME", "").strip()
APP_PASSWORD = os.environ.get("APP_PASSWORD", "").strip()


def ensure_storage():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(LOGO_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)

CONFIDENTIALITY_TEXT = (
    "The contents of this e-mail message and any attachments are confidential and are intended solely for addressee. "
    "The information may also be legally privileged. This transmission is sent in trust, for the sole purpose of delivery "
    "to the intended recipient. If you have received this transmission in error, any use, reproduction or dissemination of "
    "this transmission is strictly prohibited. If you are not the intended recipient, please immediately notify the sender "
    "by reply e-mail or phone and delete this message and its attachments, if any."
)

# --- DB helpers ---

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS recipients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_name TEXT UNIQUE NOT NULL,
            recipient_type TEXT DEFAULT 'single',
            email_to TEXT NOT NULL,
            net_terms INTEGER DEFAULT 30,
            terms_code TEXT DEFAULT 'net_30',
            location TEXT,
            frequency TEXT DEFAULT 'weekly',
            day_of_week INTEGER DEFAULT 0,
            day_of_month INTEGER DEFAULT 1,
            last_sent TEXT,
            active INTEGER DEFAULT 1,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS customer_mappings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_name TEXT UNIQUE NOT NULL,
            recipient_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(recipient_id) REFERENCES recipients(id)
        );

        CREATE TABLE IF NOT EXISTS customer_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alias_name TEXT UNIQUE NOT NULL,
            recipient_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(recipient_id) REFERENCES recipients(id)
        );

        CREATE TABLE IF NOT EXISTS group_members (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            group_id INTEGER NOT NULL,
            customer_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES recipients(id),
            FOREIGN KEY(customer_id) REFERENCES recipients(id)
        );

        CREATE TABLE IF NOT EXISTS invoice_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            path TEXT NOT NULL,
            uploaded_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS overdue_report_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_file_id INTEGER,
            invoice_path TEXT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            error TEXT,
            FOREIGN KEY(invoice_file_id) REFERENCES invoice_files(id)
        );

        CREATE TABLE IF NOT EXISTS overdue_report_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id INTEGER NOT NULL,
            group_name TEXT NOT NULL,
            terms_code TEXT,
            overdue_count INTEGER NOT NULL,
            days_overdue INTEGER NOT NULL,
            overdue_amount REAL NOT NULL,
            skipped_count INTEGER DEFAULT 0,
            skipped_invoices TEXT,
            short_paid_count INTEGER DEFAULT 0,
            short_paid_amount REAL DEFAULT 0,
            short_paid_invoices TEXT,
            FOREIGN KEY(run_id) REFERENCES overdue_report_runs(id)
        );

        CREATE TABLE IF NOT EXISTS notice_sends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            invoice_file_id INTEGER,
            invoice_path TEXT,
            recipient_id INTEGER NOT NULL,
            notice_type TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            FOREIGN KEY(invoice_file_id) REFERENCES invoice_files(id),
            FOREIGN KEY(recipient_id) REFERENCES recipients(id)
        );

        CREATE TABLE IF NOT EXISTS statement_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipient_id INTEGER NOT NULL,
            invoice_file_id INTEGER,
            run_type TEXT NOT NULL,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            sent_at TEXT,
            error TEXT,
            pdf_path TEXT,
            FOREIGN KEY(recipient_id) REFERENCES recipients(id),
            FOREIGN KEY(invoice_file_id) REFERENCES invoice_files(id)
        );

        CREATE TABLE IF NOT EXISTS scheduled_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            status TEXT NOT NULL,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            last_heartbeat TEXT,
            invoice_file_id INTEGER,
            invoice_path TEXT,
            requested_by TEXT,
            total_items INTEGER DEFAULT 0,
            processed_items INTEGER DEFAULT 0,
            sent_count INTEGER DEFAULT 0,
            skipped_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0,
            missing_email_customers TEXT DEFAULT '[]',
            error TEXT,
            FOREIGN KEY(invoice_file_id) REFERENCES invoice_files(id)
        );

        CREATE TABLE IF NOT EXISTS scheduled_job_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            recipient_id INTEGER NOT NULL,
            recipient_name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            error TEXT,
            attempts INTEGER DEFAULT 0,
            created_at TEXT NOT NULL,
            started_at TEXT,
            finished_at TEXT,
            FOREIGN KEY(job_id) REFERENCES scheduled_jobs(id),
            FOREIGN KEY(recipient_id) REFERENCES recipients(id)
        );
        """
    )
    cur.execute("PRAGMA table_info(recipients)")
    cols = [row[1] for row in cur.fetchall()]
    if "terms_code" not in cols:
        cur.execute("ALTER TABLE recipients ADD COLUMN terms_code TEXT DEFAULT 'net_30'")
    if "recipient_type" not in cols:
        cur.execute("ALTER TABLE recipients ADD COLUMN recipient_type TEXT DEFAULT 'single'")

    cur.execute("SELECT id, net_terms, terms_code FROM recipients")
    rows = cur.fetchall()
    for row in rows:
        if not row["terms_code"]:
            terms_code = normalize_terms_code(row["net_terms"]) or "net_30"
            cur.execute(
                "UPDATE recipients SET terms_code = ? WHERE id = ?",
                (terms_code, row["id"]),
            )

    cur.execute("UPDATE recipients SET recipient_type = 'single' WHERE recipient_type IS NULL OR recipient_type = ''")

    cur.execute("PRAGMA table_info(overdue_report_runs)")
    cols = [row[1] for row in cur.fetchall()]
    if cols and "invoice_path" not in cols:
        cur.execute("ALTER TABLE overdue_report_runs ADD COLUMN invoice_path TEXT")

    cur.execute("PRAGMA table_info(overdue_report_items)")
    cols = [row[1] for row in cur.fetchall()]
    if "skipped_count" not in cols:
        cur.execute("ALTER TABLE overdue_report_items ADD COLUMN skipped_count INTEGER DEFAULT 0")
    if "skipped_invoices" not in cols:
        cur.execute("ALTER TABLE overdue_report_items ADD COLUMN skipped_invoices TEXT")
    if "short_paid_count" not in cols:
        cur.execute("ALTER TABLE overdue_report_items ADD COLUMN short_paid_count INTEGER DEFAULT 0")
    if "short_paid_amount" not in cols:
        cur.execute("ALTER TABLE overdue_report_items ADD COLUMN short_paid_amount REAL DEFAULT 0")
    if "short_paid_invoices" not in cols:
        cur.execute("ALTER TABLE overdue_report_items ADD COLUMN short_paid_invoices TEXT")

    cur.execute("PRAGMA table_info(notice_sends)")
    notice_cols = [row[1] for row in cur.fetchall()]
    if "invoice_file_id" not in notice_cols:
        cur.execute("ALTER TABLE notice_sends ADD COLUMN invoice_file_id INTEGER")
    if "invoice_path" not in notice_cols:
        cur.execute("ALTER TABLE notice_sends ADD COLUMN invoice_path TEXT")

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_notice_unique "
        "ON notice_sends(invoice_file_id, invoice_path, recipient_id, notice_type)"
    )

    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_group_member_unique "
        "ON group_members(group_id, customer_id)"
    )
    cur.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_customer_alias_name "
        "ON customer_aliases(alias_name)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_customer_alias_recipient "
        "ON customer_aliases(recipient_id)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_status "
        "ON scheduled_jobs(status, created_at)"
    )
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_scheduled_job_items_job_status "
        "ON scheduled_job_items(job_id, status, id)"
    )
    cur.execute("PRAGMA table_info(scheduled_jobs)")
    scheduled_cols = [row[1] for row in cur.fetchall()]
    if scheduled_cols and "last_heartbeat" not in scheduled_cols:
        cur.execute("ALTER TABLE scheduled_jobs ADD COLUMN last_heartbeat TEXT")
    if scheduled_cols and "missing_email_customers" not in scheduled_cols:
        cur.execute("ALTER TABLE scheduled_jobs ADD COLUMN missing_email_customers TEXT DEFAULT '[]'")
    if scheduled_cols and "requested_by" not in scheduled_cols:
        cur.execute("ALTER TABLE scheduled_jobs ADD COLUMN requested_by TEXT")

    conn.commit()
    conn.close()


ensure_storage()
init_db()


def get_setting(key, default=""):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else default


def set_setting(key, value):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO settings(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value),
    )
    conn.commit()
    conn.close()


def load_upload_df(upload_file):
    if not upload_file or not upload_file.filename:
        raise RuntimeError("No file provided")

    ext = os.path.splitext(upload_file.filename)[1].lower()
    if ext not in ALLOWED_IMPORT_EXTENSIONS:
        raise RuntimeError("Unsupported file type. Use .csv or .xlsx")

    if ext == ".csv":
        return pd.read_csv(upload_file)
    return pd.read_excel(upload_file)


def safe_filename(filename):
    keep = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.")
    cleaned = "".join(ch for ch in filename if ch in keep)
    return cleaned or "logo.png"


def save_logo_file(upload_file):
    if not upload_file or not upload_file.filename:
        raise RuntimeError("No logo file provided")
    ext = os.path.splitext(upload_file.filename)[1].lower()
    if ext not in ALLOWED_LOGO_EXTENSIONS:
        raise RuntimeError("Unsupported logo type. Use PNG, JPG, or GIF.")
    os.makedirs(LOGO_DIR, exist_ok=True)
    base = safe_filename(os.path.splitext(upload_file.filename)[0])
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{base}_{timestamp}{ext}"
    path = os.path.join(LOGO_DIR, filename)
    upload_file.save(path)
    return path


def normalize_columns(df):
    df = df.copy()
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]
    return df


def get_row_value(row, keys):
    for key in keys:
        if key in row.index:
            val = row[key]
            if pd.isna(val):
                continue
            return val
    return None


def parse_int(value, default, min_value=None, max_value=None):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return default
    try:
        parsed = int(float(value))
    except Exception:
        return default
    if min_value is not None:
        parsed = max(min_value, parsed)
    if max_value is not None:
        parsed = min(max_value, parsed)
    return parsed


def parse_bool(value, default=True):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return 1 if default else 0
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"0", "false", "no", "n", "off"}:
            return 0
        if v in {"1", "true", "yes", "y", "on"}:
            return 1
    try:
        return 1 if int(value) != 0 else 0
    except Exception:
        return 1 if default else 0


def normalize_terms_code(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, (int, float)) and not pd.isna(value):
        as_int = int(float(value))
        code = f"net_{as_int}"
        return code if code in TERM_DAYS else None

    v = str(value).strip().lower()
    if not v:
        return None
    if v in TERM_LABEL_TO_CODE:
        return TERM_LABEL_TO_CODE[v]

    v = v.replace("_", " ").replace("-", " ").strip()
    if v in TERM_LABEL_TO_CODE:
        return TERM_LABEL_TO_CODE[v]

    if v.startswith("net"):
        digits = "".join(ch for ch in v if ch.isdigit())
        if digits:
            code = f"net_{digits}"
            if code in TERM_DAYS:
                return code

    if v in {"cod", "c.o.d"}:
        return "cod"
    if v in {"bill to bill", "billtobill"}:
        return "bill_to_bill"
    if v in {"month to month", "monthtomonth"}:
        return "month_to_month"
    if v in {"week to week", "weektoweek"}:
        return "week_to_week"

    return None


def get_terms_code(value, fallback="net_30"):
    code = normalize_terms_code(value)
    return code if code else fallback


def get_terms_days(terms_code):
    return TERM_DAYS.get(terms_code, 0)


def parse_ship_date(value):
    try:
        ship_dt = pd.to_datetime(value, errors="coerce")
        if not pd.isnull(ship_dt):
            return ship_dt.date()
    except Exception:
        pass
    return date.today()


def normalize_name(value):
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip()


def name_key(value):
    return normalize_name(value).lower()


def parse_email_list(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return []
    raw = str(value).replace(";", ",").replace("\n", ",")
    seen = set()
    emails = []
    for token in raw.split(","):
        email = token.strip()
        if not email:
            continue
        key = email.lower()
        if key in seen:
            continue
        seen.add(key)
        emails.append(email)
    return emails


def normalize_email_value(value):
    return ", ".join(parse_email_list(value))


def has_email_value(value):
    return bool(parse_email_list(value))


def normalize_frequency(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    v = str(value).strip().lower().replace("_", " ").replace("-", " ")
    v = " ".join(v.split())
    mapping = {
        "weekly": "weekly",
        "week": "weekly",
        "biweekly": "biweekly",
        "bi weekly": "biweekly",
        "every 2 weeks": "biweekly",
        "monthly": "monthly",
        "month": "monthly",
        "none": "none",
        "off": "none",
    }
    return mapping.get(v)


def parse_day_of_week_value(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    if isinstance(value, str):
        v = value.strip().lower()
        if not v:
            return None
        day_map = {
            "monday": 0,
            "mon": 0,
            "tuesday": 1,
            "tue": 1,
            "wednesday": 2,
            "wed": 2,
            "thursday": 3,
            "thu": 3,
            "friday": 4,
            "fri": 4,
            "saturday": 5,
            "sat": 5,
            "sunday": 6,
            "sun": 6,
        }
        if v in day_map:
            return day_map[v]
    return parse_int(value, None, 0, 6)


def default_schedule_for_terms(terms_code):
    if terms_code == "month_to_month":
        return "monthly", 0, 1
    return "weekly", 0, 1


def ensure_recipient_email(recipient, provided_email=""):
    current = normalize_email_value(recipient.get("email_to", ""))
    if current:
        recipient["email_to"] = current
        return recipient

    provided = normalize_email_value(provided_email)
    if not provided:
        raise RuntimeError("Missing recipient email")

    conn = get_db()
    cur = conn.cursor()
    cur.execute("UPDATE recipients SET email_to = ? WHERE id = ?", (provided, recipient["id"]))
    conn.commit()
    conn.close()
    recipient["email_to"] = provided
    return recipient


def auth_enabled():
    return bool(APP_PASSWORD)


@app.before_request
def enforce_login():
    ensure_schedule_worker_running()
    if not auth_enabled():
        return
    if request.endpoint in {"login", "static", "app_logo"}:
        return
    if request.path.startswith("/static"):
        return
    if session.get("logged_in"):
        return
    return redirect(url_for("login", next=request.path))


def html_escape(value):
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def normalize_cc(value):
    if not value:
        return ""
    value = str(value).strip()
    if not value:
        return ""
    for sep in [",", ";"]:
        if sep in value:
            value = value.split(sep)[0].strip()
            break
    return value


def get_notice_cc(notice_type):
    key_map = {
        "statement": "cc_statement",
        "overdue": "cc_overdue",
        "skipped": "cc_skipped",
        "short_paid": "cc_short_paid",
    }
    setting_key = key_map.get(notice_type)
    if not setting_key:
        return None
    return normalize_cc(get_setting(setting_key, "")) or None


def get_email_template_body(template_key):
    default = EMAIL_TEMPLATE_DEFAULTS.get(template_key, "")
    setting_key = f"email_template_{template_key}"
    return get_setting(setting_key, default)


def build_signature(logo_html):
    company_name = get_setting("company_name", "").strip()
    company_address = get_setting("company_address", "").strip()
    company_phone = get_setting("company_phone", "").strip()
    company_email = get_setting("company_email", "").strip()
    company_website = get_setting("company_website", "").strip()

    text_lines = ["--"]
    if company_name:
        text_lines.append(company_name)
    if company_address:
        text_lines.append(company_address)
    if company_website:
        text_lines.append(company_website)
    if company_email:
        text_lines.append(company_email)
    if company_phone:
        text_lines.append(f"Tel: {company_phone}")
    text_lines.append("")
    text_lines.append("Statement of Confidentiality")
    text_lines.append(CONFIDENTIALITY_TEXT)

    html_parts = [f'<span>-- </span><br><div dir="ltr">']
    if logo_html:
        html_parts.append(f"<div>{logo_html}</div>")
    if company_name:
        html_parts.append(
            f'<div><b><font size="4" color="#000000">{html_escape(company_name)}</font></b></div>'
        )
    if company_address:
        html_parts.append(f"<div><font>{html_escape(company_address)}</font></div>")
    if company_website:
        website_safe = html_escape(company_website)
        html_parts.append(
            f'<div><span style="color:rgb(34,34,34);font-family:Calibri,sans-serif">'
            f'<a href="{website_safe}" target="_blank" rel="noopener">{website_safe}</a>'
            f"</span></div>"
        )
    if company_email:
        email_safe = html_escape(company_email)
        html_parts.append(
            f'<div><a href="mailto:{email_safe}" target="_blank" rel="noopener">{email_safe}</a><br></div>'
        )
    if company_phone:
        html_parts.append(f"<div>Tel: {html_escape(company_phone)}</div>")

    html_parts.append(
        '<div><div><div style="color:rgb(34,34,34)">'
        '<p style="color:rgb(0,0,0);font-size:12.7273px;margin:0in 0in 0.0001pt">'
        '<u><font color="#444444">Statement of Confidentiality</font></u></p>'
        f'<p style="color:rgb(0,0,0);font-size:12.7273px;margin:0in 0in 0.0001pt">'
        f'<font color="#444444">{html_escape(CONFIDENTIALITY_TEXT)}</font></p>'
        "</div></div></div>"
    )
    html_parts.append("</div>")

    signature_text = "\n".join(text_lines).strip()
    signature_html = "".join(html_parts)
    return signature_text, signature_html


def compute_due_date(ship_date, terms_code):
    if terms_code in TERM_DAYS:
        return ship_date + timedelta(days=TERM_DAYS[terms_code])
    if terms_code == "week_to_week":
        week_start = ship_date - timedelta(days=ship_date.weekday())
        return week_start + timedelta(days=4)
    if terms_code == "month_to_month":
        first_of_month = ship_date.replace(day=1)
        next_month = first_of_month + timedelta(days=32)
        return next_month.replace(day=1)
    return ship_date + timedelta(days=30)


def compute_status(today, due_date):
    if today > due_date:
        return "Overdue"
    if 0 <= (due_date - today).days <= 7:
        return "Due This Week"
    return "Unpaid"


def parse_datetime(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def get_latest_overdue_run():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT r.*, f.filename FROM overdue_report_runs r "
        "LEFT JOIN invoice_files f ON f.id = r.invoice_file_id "
        "ORDER BY r.created_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_overdue_items(run_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM overdue_report_items WHERE run_id = ? ORDER BY overdue_amount DESC",
        (run_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def resolve_invoice_file_id(invoice_path):
    if not invoice_path:
        return None
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM invoice_files WHERE path = ?", (invoice_path,))
    row = cur.fetchone()
    conn.close()
    return row["id"] if row else None


def notice_run_id_required():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(notice_sends)")
    rows = cur.fetchall()
    conn.close()
    for row in rows:
        if row[1] == "run_id":
            return bool(row[3])
    return False


def record_notice_send(run_id, invoice_file_id, invoice_path, recipient_id, notice_type):
    if not recipient_id:
        return
    if not invoice_file_id and invoice_path:
        invoice_file_id = resolve_invoice_file_id(invoice_path)
    if notice_run_id_required() and not run_id:
        run_id = resolve_run_id(None)
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        if notice_run_id_required():
            cur.execute(
                "INSERT INTO notice_sends(run_id, invoice_file_id, invoice_path, recipient_id, notice_type, sent_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (run_id, invoice_file_id, invoice_path, recipient_id, notice_type, now),
            )
        else:
            cur.execute(
                "INSERT INTO notice_sends(invoice_file_id, invoice_path, recipient_id, notice_type, sent_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (invoice_file_id, invoice_path, recipient_id, notice_type, now),
            )
        conn.commit()
    except sqlite3.IntegrityError:
        pass
    except sqlite3.OperationalError:
        conn.close()
        init_db()
        conn = get_db()
        cur = conn.cursor()
        try:
            if notice_run_id_required():
                cur.execute(
                    "INSERT INTO notice_sends(run_id, invoice_file_id, invoice_path, recipient_id, notice_type, sent_at) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (run_id, invoice_file_id, invoice_path, recipient_id, notice_type, now),
                )
            else:
                cur.execute(
                    "INSERT INTO notice_sends(invoice_file_id, invoice_path, recipient_id, notice_type, sent_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (invoice_file_id, invoice_path, recipient_id, notice_type, now),
                )
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            conn.close()
        return
    finally:
        conn.close()


def get_notice_sends(invoice_file_id, invoice_path):
    if not invoice_file_id and not invoice_path:
        return set()
    conn = get_db()
    cur = conn.cursor()
    try:
        if invoice_file_id:
            cur.execute(
                "SELECT recipient_id, notice_type FROM notice_sends WHERE invoice_file_id = ?",
                (invoice_file_id,),
            )
        else:
            cur.execute(
                "SELECT recipient_id, notice_type FROM notice_sends WHERE invoice_path = ?",
                (invoice_path,),
            )
        rows = cur.fetchall()
    except sqlite3.OperationalError:
        rows = []
    conn.close()
    return {(row["recipient_id"], row["notice_type"]) for row in rows}


def resolve_run_id(run_id):
    if run_id:
        try:
            return int(run_id)
        except Exception:
            return None
    latest = get_latest_overdue_run()
    return latest["id"] if latest else None


def get_overdue_run(run_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT r.*, f.filename FROM overdue_report_runs r "
        "LEFT JOIN invoice_files f ON f.id = r.invoice_file_id "
        "WHERE r.id = ?",
        (run_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def save_overdue_report(rows, invoice_file_id, invoice_path, status, error=None):
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "INSERT INTO overdue_report_runs(invoice_file_id, invoice_path, status, created_at, error) "
        "VALUES (?, ?, ?, ?, ?)",
        (invoice_file_id, invoice_path, status, now, error),
    )
    run_id = cur.lastrowid

    if status == "success":
        payload = []
        for row in rows:
            skipped_json = json.dumps(row.get("skipped_invoices", []))
            short_paid_json = json.dumps(row.get("short_paid_invoices", []))
            payload.append(
                (
                    run_id,
                    row["group_name"],
                    row.get("terms_code"),
                    int(row["overdue_count"]),
                    int(row["days_overdue"]),
                    float(row["overdue_amount"]),
                    int(row.get("skipped_count", 0)),
                    skipped_json,
                    int(row.get("short_paid_count", 0)),
                    float(row.get("short_paid_amount", 0.0)),
                    short_paid_json,
                )
            )
        if payload:
            cur.executemany(
                "INSERT INTO overdue_report_items(run_id, group_name, terms_code, overdue_count, days_overdue, overdue_amount, skipped_count, skipped_invoices, short_paid_count, short_paid_amount, short_paid_invoices) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                payload,
            )

    conn.commit()
    conn.close()
    return run_id


def run_overdue_report():
    invoice_file_id = None
    invoice_path = None
    try:
        invoice_file_id, invoice_path = get_invoice_for_run()
        rows = compute_overdue_report(invoice_path)
        save_overdue_report(rows, invoice_file_id, invoice_path, "success")
        return "success", None
    except Exception as exc:
        save_overdue_report([], invoice_file_id, invoice_path, "error", str(exc))
        return "error", str(exc)


def get_recipients_terms_map():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, group_name, terms_code, net_terms, recipient_type, email_to FROM recipients WHERE active = 1"
    )
    rows = cur.fetchall()
    conn.close()
    recipients_map = {}
    for row in rows:
        terms_code = row["terms_code"] or normalize_terms_code(row["net_terms"]) or "net_30"
        recipients_map[row["group_name"]] = {
            "id": row["id"],
            "terms_code": terms_code,
            "recipient_type": row["recipient_type"] or "single",
            "has_email": has_email_value(row["email_to"]),
        }
    return recipients_map


def get_single_recipients_map():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, group_name, terms_code, net_terms FROM recipients "
        "WHERE recipient_type = 'single' AND active = 1"
    )
    rows = cur.fetchall()
    conn.close()
    aliases_by_id = get_alias_names_by_recipient_ids([row["id"] for row in rows])
    mapping = {}
    for row in rows:
        terms_code = row["terms_code"] or normalize_terms_code(row["net_terms"]) or "net_30"
        keys = []
        base_key = name_key(row["group_name"])
        if base_key:
            keys.append(base_key)
        for alias_name in aliases_by_id.get(row["id"], []):
            alias_key = name_key(alias_name)
            if alias_key:
                keys.append(alias_key)
        payload = {
            "id": row["id"],
            "group_name": row["group_name"],
            "terms_code": terms_code,
        }
        for key in keys:
            mapping[key] = payload
    return mapping


def get_group_membership_map():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT c.id AS customer_id, c.group_name AS customer_name, g.id AS group_id, g.group_name AS group_name, "
        "g.terms_code, g.net_terms "
        "FROM group_members gm "
        "JOIN recipients c ON c.id = gm.customer_id "
        "JOIN recipients g ON g.id = gm.group_id "
        "WHERE g.recipient_type = 'group' AND g.active = 1 AND c.active = 1"
    )
    rows = cur.fetchall()
    conn.close()
    aliases_by_id = get_alias_names_by_recipient_ids([row["customer_id"] for row in rows])
    mapping = {}
    for row in rows:
        terms_code = row["terms_code"] or normalize_terms_code(row["net_terms"]) or "net_30"
        keys = []
        base_key = name_key(row["customer_name"])
        if base_key:
            keys.append(base_key)
        for alias_name in aliases_by_id.get(row["customer_id"], []):
            alias_key = name_key(alias_name)
            if alias_key:
                keys.append(alias_key)
        payload = {
            "group_id": row["group_id"],
            "group_name": row["group_name"],
            "terms_code": terms_code,
        }
        for key in keys:
            mapping[key] = payload
    return mapping


def get_group_members_by_group_id():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT gm.group_id, c.id AS customer_id, c.group_name AS customer_name "
        "FROM group_members gm "
        "JOIN recipients c ON c.id = gm.customer_id"
    )
    rows = cur.fetchall()
    conn.close()
    members = {}
    for row in rows:
        members.setdefault(row["group_id"], []).append(
            {"id": row["customer_id"], "name": row["customer_name"]}
        )
    return members


def get_grouped_customer_ids():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT customer_id FROM group_members")
    rows = cur.fetchall()
    conn.close()
    return {row["customer_id"] for row in rows}


def get_alias_names_by_recipient_ids(recipient_ids):
    if not recipient_ids:
        return {}
    ids = [int(rid) for rid in recipient_ids if rid is not None]
    if not ids:
        return {}

    placeholders = ",".join(["?"] * len(ids))
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        f"SELECT recipient_id, alias_name FROM customer_aliases WHERE recipient_id IN ({placeholders})",
        ids,
    )
    rows = cur.fetchall()
    conn.close()

    aliases = {}
    for row in rows:
        alias_name = normalize_name(row["alias_name"])
        if not alias_name:
            continue
        aliases.setdefault(row["recipient_id"], []).append(alias_name)
    return aliases


def get_all_single_name_keys():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, group_name FROM recipients WHERE recipient_type = 'single'")
    singles = cur.fetchall()
    aliases_by_id = {}
    ids = [row["id"] for row in singles]
    if ids:
        placeholders = ",".join(["?"] * len(ids))
        cur.execute(
            f"SELECT recipient_id, alias_name FROM customer_aliases WHERE recipient_id IN ({placeholders})",
            ids,
        )
        for row in cur.fetchall():
            alias_name = normalize_name(row["alias_name"])
            if not alias_name:
                continue
            aliases_by_id.setdefault(row["recipient_id"], []).append(alias_name)
    conn.close()

    keys = set()
    for row in singles:
        base = name_key(row["group_name"])
        if base:
            keys.add(base)
        for alias_name in aliases_by_id.get(row["id"], []):
            alias_key = name_key(alias_name)
            if alias_key:
                keys.add(alias_key)
    return keys


def get_dashboard_terms_lookup(include_excluded=False):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, group_name, active, terms_code, net_terms FROM recipients WHERE recipient_type = 'single'"
    )
    singles = cur.fetchall()
    single_ids = [row["id"] for row in singles]
    aliases_by_id = {}
    if single_ids:
        placeholders = ",".join(["?"] * len(single_ids))
        cur.execute(
            f"SELECT recipient_id, alias_name FROM customer_aliases WHERE recipient_id IN ({placeholders})",
            single_ids,
        )
        for row in cur.fetchall():
            alias_name = normalize_name(row["alias_name"])
            if not alias_name:
                continue
            aliases_by_id.setdefault(row["recipient_id"], []).append(alias_name)
    cur.execute(
        "SELECT c.id AS customer_id, c.group_name AS customer_name, c.active AS customer_active, "
        "g.active AS group_active, g.terms_code AS group_terms_code, g.net_terms AS group_net_terms "
        "FROM group_members gm "
        "JOIN recipients c ON c.id = gm.customer_id "
        "JOIN recipients g ON g.id = gm.group_id "
        "WHERE c.recipient_type = 'single' AND g.recipient_type = 'group'"
    )
    grouped = cur.fetchall()
    conn.close()

    lookup = {}
    excluded = set()
    grouped_customer_ids = set()

    for row in grouped:
        customer_id = row["customer_id"]
        grouped_customer_ids.add(customer_id)
        keys = []
        base_key = name_key(row["customer_name"])
        if base_key:
            keys.append(base_key)
        for alias_name in aliases_by_id.get(customer_id, []):
            alias_key = name_key(alias_name)
            if alias_key:
                keys.append(alias_key)
        if not row["customer_active"] or not row["group_active"]:
            for key in keys:
                excluded.add(key)
                lookup.pop(key, None)
            continue
        terms_code = (
            row["group_terms_code"]
            or normalize_terms_code(row["group_net_terms"])
            or "net_30"
        )
        for key in keys:
            lookup[key] = terms_code
            excluded.discard(key)

    for row in singles:
        customer_id = row["id"]
        if customer_id in grouped_customer_ids:
            continue
        keys = []
        base_key = name_key(row["group_name"])
        if base_key:
            keys.append(base_key)
        for alias_name in aliases_by_id.get(customer_id, []):
            alias_key = name_key(alias_name)
            if alias_key:
                keys.append(alias_key)
        if not row["active"]:
            for key in keys:
                excluded.add(key)
            continue
        terms_code = row["terms_code"] or normalize_terms_code(row["net_terms"]) or "net_30"
        for key in keys:
            lookup[key] = terms_code

    if include_excluded:
        return lookup, excluded
    return lookup


def get_terms_distribution(include_customers=False):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT s.group_name AS customer_name, s.terms_code AS single_terms_code, s.net_terms AS single_net_terms, "
        "g.terms_code AS group_terms_code, g.net_terms AS group_net_terms "
        "FROM recipients s "
        "LEFT JOIN group_members gm ON gm.customer_id = s.id "
        "LEFT JOIN recipients g ON g.id = gm.group_id "
        "WHERE s.recipient_type = 'single' "
        "AND s.active = 1 "
        "AND (gm.group_id IS NULL OR (g.recipient_type = 'group' AND g.active = 1))"
    )
    rows = cur.fetchall()
    conn.close()

    counts = {}
    customers_by_terms = {}
    seen_customers = set()
    for row in rows:
        customer_name = normalize_name(row["customer_name"])
        customer_key = name_key(customer_name)
        if not customer_key or customer_key in seen_customers:
            continue
        seen_customers.add(customer_key)

        terms_code = row["group_terms_code"] or row["single_terms_code"]
        if not terms_code:
            terms_code = normalize_terms_code(row["group_net_terms"])
        if not terms_code:
            terms_code = normalize_terms_code(row["single_net_terms"])
        if not terms_code:
            terms_code = "net_30"
        counts[terms_code] = counts.get(terms_code, 0) + 1
        if customer_name:
            bucket = customers_by_terms.setdefault(terms_code, {})
            bucket[customer_key] = customer_name

    if not include_customers:
        return counts

    customers_sorted = {
        code: sorted(names.values(), key=lambda n: n.lower())
        for code, names in customers_by_terms.items()
    }
    return counts, customers_sorted


def build_pie_chart(segments):
    total = sum(float(value) for _, value, _ in segments)
    if total <= 0:
        return {
            "has_data": False,
            "gradient": "#d9d9d9",
            "legend": [],
            "labels": [],
            "total": 0,
        }

    start = 0.0
    parts = []
    legend = []
    labels = []
    for label, value, color in segments:
        value = float(value)
        if value <= 0:
            continue
        pct = (value / total) * 100
        mid = start + (pct / 2.0)
        end = start + pct
        parts.append(f"{color} {start:.2f}% {end:.2f}%")
        legend.append(
            {
                "label": label,
                "value": value,
                "color": color,
                "percent": pct,
            }
        )
        if pct >= 5.0:
            angle = ((mid / 100.0) * 360.0) - 90.0
            rad = math.radians(angle)
            labels.append(
                {
                    "label": label,
                    "text": f"{pct:.1f}%",
                    "x": 50.0 + (math.cos(rad) * 32.0),
                    "y": 50.0 + (math.sin(rad) * 32.0),
                    "color": color,
                }
            )
        start = end

    if not parts:
        return {
            "has_data": False,
            "gradient": "#d9d9d9",
            "legend": [],
            "labels": [],
            "total": 0,
        }
    return {
        "has_data": True,
        "gradient": f"conic-gradient({', '.join(parts)})",
        "legend": legend,
        "labels": labels,
        "total": total,
    }


def build_treemap_chart(segments):
    total = 0.0
    for segment in segments:
        if len(segment) == 4:
            _, _, value, _ = segment
        else:
            _, value, _ = segment
        total += float(value)
    if total <= 0:
        return {"has_data": False, "tiles": [], "legend": [], "total": 0}

    items = []
    for segment in segments:
        if len(segment) == 4:
            code, label, value, color = segment
        else:
            label, value, color = segment
            code = label
        value = float(value)
        if value <= 0:
            continue
        items.append(
            {
                "code": code,
                "label": label,
                "value": value,
                "color": color,
                "percent": (value / total) * 100.0,
            }
        )
    if not items:
        return {"has_data": False, "tiles": [], "legend": [], "total": 0}

    items.sort(key=lambda i: i["value"], reverse=True)
    tiles = []

    def split_slice(data, x, y, w, h):
        if not data:
            return
        if len(data) == 1:
            item = data[0]
            area_pct = (item["value"] / total) * 100.0
            tiles.append(
                {
                    "x": x,
                    "y": y,
                    "w": w,
                    "h": h,
                    "code": item["code"],
                    "label": item["label"],
                    "value": item["value"],
                    "color": item["color"],
                    "percent": item["percent"],
                    "area_percent": area_pct,
                    "show_label": w >= 9.0 and h >= 9.0,
                }
            )
            return

        subtotal = sum(item["value"] for item in data)
        half = subtotal / 2.0
        running = 0.0
        split_index = 0
        for idx, item in enumerate(data):
            running += item["value"]
            split_index = idx
            if running >= half:
                break

        group_a = data[: split_index + 1]
        group_b = data[split_index + 1 :]
        if not group_b:
            # Guard for degenerate split; enforce at least one item in each group.
            group_a = data[:1]
            group_b = data[1:]

        sum_a = sum(item["value"] for item in group_a)
        ratio_a = sum_a / subtotal if subtotal > 0 else 0.0

        if w >= h:
            w_a = w * ratio_a
            split_slice(group_a, x, y, w_a, h)
            split_slice(group_b, x + w_a, y, w - w_a, h)
        else:
            h_a = h * ratio_a
            split_slice(group_a, x, y, w, h_a)
            split_slice(group_b, x, y + h_a, w, h - h_a)

    split_slice(items, 0.0, 0.0, 100.0, 100.0)

    legend = [
        {
            "code": item["code"],
            "label": item["label"],
            "value": item["value"],
            "color": item["color"],
            "percent": item["percent"],
        }
        for item in items
    ]

    return {"has_data": True, "tiles": tiles, "legend": legend, "total": total}


def compute_dashboard_financials():
    result = {
        "total_receivable": 0.0,
        "overdue_amount": 0.0,
        "current_amount": 0.0,
        "invoice_label": None,
        "error": None,
    }
    try:
        invoice_file_id, invoice_path = get_invoice_for_run()
        df = load_invoice_df(invoice_path)
    except Exception as exc:
        result["error"] = str(exc)
        return result

    if invoice_file_id:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT filename FROM invoice_files WHERE id = ?", (invoice_file_id,))
        row = cur.fetchone()
        conn.close()
        if row and row["filename"]:
            result["invoice_label"] = row["filename"]
    if not result["invoice_label"]:
        result["invoice_label"] = os.path.basename(invoice_path)

    terms_lookup, excluded_customer_keys = get_dashboard_terms_lookup(include_excluded=True)
    open_items = []
    total_receivable = 0.0

    for _, row in df.iterrows():
        val_total = pd.to_numeric(row.get("Order Total"), errors="coerce")
        val_paid = pd.to_numeric(row.get("Paid Amount", 0), errors="coerce")
        amt = 0.0 if pd.isna(val_total) else float(val_total)
        paid = 0.0 if pd.isna(val_paid) else float(val_paid)
        outstanding = amt - paid
        if outstanding <= 0.01:
            continue

        customer = normalize_name(row.get("Customer Name"))
        customer_key = name_key(customer)
        if customer_key in excluded_customer_keys:
            continue
        terms_code = terms_lookup.get(customer_key, "net_30")
        ship_date = parse_ship_date(row.get("Shipping Date"))
        total_receivable += outstanding
        open_items.append(
            {
                "customer_key": customer_key,
                "terms_code": terms_code,
                "ship_date": ship_date,
                "outstanding": outstanding,
            }
        )

    today = date.today()
    overdue_amount = 0.0
    grouped = {}
    for item in open_items:
        grouped.setdefault(item["customer_key"], []).append(item)

    for items in grouped.values():
        if not items:
            continue
        terms_code = items[0]["terms_code"] or "net_30"
        if terms_code == "bill_to_bill":
            sorted_items = sorted(items, key=lambda i: i["ship_date"])
            for idx, item in enumerate(sorted_items):
                ship_date = item["ship_date"]
                if idx < len(sorted_items) - 1:
                    due_date = sorted_items[idx + 1]["ship_date"]
                else:
                    due_date = ship_date + timedelta(days=15)
                if today > due_date:
                    overdue_amount += item["outstanding"]
        else:
            for item in items:
                due_date = compute_due_date(item["ship_date"], terms_code)
                if today > due_date:
                    overdue_amount += item["outstanding"]

    result["total_receivable"] = total_receivable
    result["overdue_amount"] = overdue_amount
    result["current_amount"] = max(total_receivable - overdue_amount, 0.0)
    return result


def compute_overdue_report(invoice_path):
    df = load_invoice_df(invoice_path)
    group_map = get_group_membership_map()
    single_map = get_single_recipients_map()
    today = date.today()

    grouped = {}
    group_invoices = {}

    for _, row in df.iterrows():
        val_total = pd.to_numeric(row.get("Order Total"), errors="coerce")
        val_paid = pd.to_numeric(row.get("Paid Amount", 0), errors="coerce")
        amt = 0.0 if pd.isna(val_total) else float(val_total)
        paid = 0.0 if pd.isna(val_paid) else float(val_paid)
        outstanding = amt - paid

        customer_name = normalize_name(row.get("Customer Name"))
        if not customer_name:
            continue
        key = name_key(customer_name)
        group_entry = group_map.get(key)
        if group_entry:
            group_name = group_entry["group_name"]
            terms_code = group_entry["terms_code"]
            location = customer_name
        else:
            single_entry = single_map.get(key)
            if not single_entry:
                continue
            group_name = single_entry["group_name"]
            terms_code = single_entry["terms_code"]
            # Singles (including merged aliases) should stay under one location bucket.
            location = group_name
        ship_date = parse_ship_date(row.get("Shipping Date"))
        order_id = row.get("Order ID")
        try:
            order_id = str(int(float(order_id)))
        except Exception:
            order_id = str(order_id)
        if amt <= 0:
            continue

        group_invoices.setdefault(group_name, {})
        paid_amount = max(0.0, amt - max(outstanding, 0.0))
        fully_paid = outstanding <= 0.01
        short_paid = paid_amount > 0.01 and outstanding > 0.01
        unpaid = paid_amount <= 0.01 and outstanding > 0.01

        group_invoices[group_name].setdefault(location, [])
        group_invoices[group_name][location].append(
            {
                "order_id": order_id,
                "ship_date": ship_date,
                "outstanding": outstanding,
                "paid_amount": paid_amount,
                "fully_paid": fully_paid,
                "short_paid": short_paid,
                "unpaid": unpaid,
                "location": location,
            }
        )

        if outstanding <= 0:
            continue

        grouped.setdefault(group_name, {"terms_code": terms_code, "items": []})
        grouped[group_name]["items"].append(
            {
                "ship_date": ship_date,
                "outstanding": outstanding,
            }
        )

    report = []
    for group_name, data in grouped.items():
        items = data["items"]
        terms_code = data["terms_code"]

        overdue_items = []
        if terms_code == "bill_to_bill":
            sorted_items = sorted(items, key=lambda i: i["ship_date"])
            if not sorted_items:
                continue
            for idx, item in enumerate(sorted_items):
                ship_date = item["ship_date"]
                if idx < len(sorted_items) - 1:
                    due_date = sorted_items[idx + 1]["ship_date"]
                else:
                    due_date = ship_date + timedelta(days=15)
                if today > due_date:
                    overdue_items.append(
                        {
                            "due_date": due_date,
                            "outstanding": item["outstanding"],
                        }
                    )
        else:
            for item in items:
                due_date = compute_due_date(item["ship_date"], terms_code)
                if today > due_date:
                    overdue_items.append(
                        {
                            "due_date": due_date,
                            "outstanding": item["outstanding"],
                        }
                    )

        overdue_count = len(overdue_items)
        overdue_amount = sum(i["outstanding"] for i in overdue_items)
        if overdue_items:
            oldest_due_date = min(i["due_date"] for i in overdue_items)
            days_overdue = (today - oldest_due_date).days
        else:
            days_overdue = 0

        skipped_list = []
        short_paid_list = []
        short_paid_count = 0
        short_paid_amount = 0.0
        location_map = group_invoices.get(group_name, {})

        for location, invoices in location_map.items():
            for inv in invoices:
                if inv["short_paid"]:
                    short_paid_count += 1
                    short_paid_amount += inv["outstanding"]
                    short_paid_list.append(
                        {
                            "order_id": inv["order_id"],
                            "ship_date": inv["ship_date"].strftime("%m/%d/%Y"),
                            "location": location,
                            "amount": inv["outstanding"],
                        }
                    )

        for location, invoices in location_map.items():
            paid_dates = [i["ship_date"] for i in invoices if i["fully_paid"]]
            if not paid_dates:
                continue
            max_paid_date = max(paid_dates)
            for inv in invoices:
                if inv["unpaid"] and inv["ship_date"] < max_paid_date:
                    skipped_list.append(
                        {
                            "order_id": inv["order_id"],
                            "ship_date": inv["ship_date"].strftime("%m/%d/%Y"),
                            "location": location,
                        }
                    )

        if overdue_count == 0 and len(skipped_list) == 0 and short_paid_count == 0:
            continue

        report.append(
            {
                "group_name": group_name,
                "terms_code": terms_code,
                "overdue_count": overdue_count,
                "overdue_amount": overdue_amount,
                "days_overdue": days_overdue,
                "skipped_count": len(skipped_list),
                "skipped_invoices": skipped_list,
                "short_paid_count": short_paid_count,
                "short_paid_amount": short_paid_amount,
                "short_paid_invoices": short_paid_list,
            }
        )

    report.sort(key=lambda x: x["overdue_amount"], reverse=True)
    return report


def import_recipients_from_df(df):
    df = normalize_columns(df)
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    added = 0
    updated = 0
    skipped = 0

    for _, row in df.iterrows():
        group_name = get_row_value(row, ["group_name", "group", "customer_group"])
        email_to = get_row_value(row, ["email_to", "email", "emails", "email_address"])

        if not group_name or not email_to:
            skipped += 1
            continue

        group_name = str(group_name).strip()
        email_to = str(email_to).strip()
        if not group_name or not email_to:
            skipped += 1
            continue

        terms_value = get_row_value(row, ["terms", "terms_code", "payment_terms"])
        net_terms_value = get_row_value(row, ["net_terms", "terms_days", "net_days"])
        terms_code = normalize_terms_code(terms_value) or normalize_terms_code(net_terms_value) or "net_30"
        net_terms = get_terms_days(terms_code)
        location = get_row_value(row, ["location"])
        location = "" if location is None or pd.isna(location) else str(location).strip()

        frequency = get_row_value(row, ["frequency"])
        frequency = str(frequency).strip().lower() if frequency else "weekly"
        if frequency not in RECIPIENT_FREQUENCIES:
            frequency = "weekly"

        day_of_week = parse_int(get_row_value(row, ["day_of_week", "weekday"]), 0, 0, 6)
        day_of_month = parse_int(get_row_value(row, ["day_of_month"]), 1, 1, 28)
        active = parse_bool(get_row_value(row, ["active", "enabled"]), True)

        cur.execute("SELECT id FROM recipients WHERE group_name = ?", (group_name,))
        existing = cur.fetchone()
        if existing:
            cur.execute(
                "UPDATE recipients SET email_to = ?, net_terms = ?, terms_code = ?, location = ?, frequency = ?, "
                "day_of_week = ?, day_of_month = ?, active = ? WHERE id = ?",
                (email_to, net_terms, terms_code, location, frequency, day_of_week, day_of_month, active, existing["id"]),
            )
            updated += 1
        else:
            cur.execute(
                "INSERT INTO recipients(group_name, email_to, net_terms, terms_code, location, frequency, day_of_week, "
                "day_of_month, active, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (group_name, email_to, net_terms, terms_code, location, frequency, day_of_week, day_of_month, active, now),
            )
            added += 1

    conn.commit()
    conn.close()
    return added, updated, skipped


def import_mappings_from_df(df):
    df = normalize_columns(df)
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    added = 0
    updated = 0
    skipped = 0
    missing_groups = set()

    for _, row in df.iterrows():
        customer_name = get_row_value(row, ["customer_name", "customer"])
        if not customer_name:
            skipped += 1
            continue
        customer_name = str(customer_name).strip()
        if not customer_name:
            skipped += 1
            continue

        recipient_id = get_row_value(row, ["recipient_id"])
        if recipient_id is not None and not pd.isna(recipient_id):
            recipient_id = parse_int(recipient_id, None)
        else:
            recipient_id = None

        if not recipient_id:
            group_name = get_row_value(row, ["group_name", "group"])
            if not group_name:
                skipped += 1
                continue
            group_name = str(group_name).strip()
            cur.execute("SELECT id FROM recipients WHERE group_name = ?", (group_name,))
            recipient_row = cur.fetchone()
            if not recipient_row:
                missing_groups.add(group_name)
                skipped += 1
                continue
            recipient_id = recipient_row["id"]
        else:
            cur.execute("SELECT id FROM recipients WHERE id = ?", (recipient_id,))
            if not cur.fetchone():
                skipped += 1
                continue

        cur.execute("SELECT id FROM customer_mappings WHERE customer_name = ?", (customer_name,))
        existing = cur.fetchone()
        if existing:
            cur.execute(
                "UPDATE customer_mappings SET recipient_id = ? WHERE id = ?",
                (recipient_id, existing["id"]),
            )
            updated += 1
        else:
            cur.execute(
                "INSERT INTO customer_mappings(customer_name, recipient_id, created_at) VALUES (?, ?, ?)",
                (customer_name, recipient_id, now),
            )
            added += 1

    conn.commit()
    conn.close()
    return added, updated, skipped, sorted(missing_groups)


def import_bulk_customers_from_upload(upload_file):
    df = load_upload_df(upload_file)
    df = normalize_columns(df)

    required = {"customer_name", "terms"}
    missing_cols = sorted(required - set(df.columns))
    if missing_cols:
        raise RuntimeError(f"Missing required columns: {', '.join(missing_cols)}")

    has_email_col = any(col in df.columns for col in ["email_to", "email", "emails", "email_address"])
    has_frequency_col = "frequency" in df.columns
    has_dow_col = any(col in df.columns for col in ["day_of_week", "weekday"])
    has_dom_col = "day_of_month" in df.columns

    latest_keys = set()
    try:
        latest_names, _ = get_latest_invoice_customer_names()
        latest_keys = {name_key(name) for name in latest_names}
    except Exception:
        latest_keys = set()

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM recipients ORDER BY id ASC")
    recipients = [dict(row) for row in cur.fetchall()]

    single_by_key = {}
    single_by_id = {}
    group_name_keys = set()
    for rec in recipients:
        key = name_key(rec["group_name"])
        if rec["recipient_type"] == "single":
            single_by_key[key] = rec
            single_by_id[rec["id"]] = rec
        else:
            group_name_keys.add(key)

    alias_lookup = {}
    if single_by_id:
        placeholders = ",".join(["?"] * len(single_by_id))
        cur.execute(
            f"SELECT alias_name, recipient_id FROM customer_aliases WHERE recipient_id IN ({placeholders})",
            list(single_by_id.keys()),
        )
        for row in cur.fetchall():
            alias_key = name_key(row["alias_name"])
            rec = single_by_id.get(row["recipient_id"])
            if alias_key and rec:
                alias_lookup[alias_key] = rec

    allowed_keys = set(single_by_key.keys()) | set(alias_lookup.keys()) | latest_keys
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    added = 0
    updated = 0
    skipped = 0
    skipped_details = []

    for idx, row in df.iterrows():
        row_no = idx + 2
        customer_name = normalize_name(get_row_value(row, ["customer_name"]))
        if not customer_name:
            skipped += 1
            skipped_details.append(f"Row {row_no}: missing customer name")
            continue
        key = name_key(customer_name)

        if key not in allowed_keys:
            skipped += 1
            skipped_details.append(f"Row {row_no}: customer not in New or All lists")
            continue

        terms_code = normalize_terms_code(get_row_value(row, ["terms"]))
        if not terms_code:
            skipped += 1
            skipped_details.append(f"Row {row_no}: invalid terms value")
            continue
        net_terms = get_terms_days(terms_code)

        existing = single_by_key.get(key) or alias_lookup.get(key)
        if existing:
            updates = ["terms_code = ?", "net_terms = ?"]
            values = [terms_code, net_terms]

            if has_email_col:
                email_to = normalize_email_value(
                    get_row_value(row, ["email_to", "email", "emails", "email_address"])
                )
                if email_to:
                    updates.append("email_to = ?")
                    values.append(email_to)

            if has_frequency_col:
                frequency = normalize_frequency(get_row_value(row, ["frequency"]))
                if frequency:
                    updates.append("frequency = ?")
                    values.append(frequency)

            if has_dow_col:
                day_of_week = parse_day_of_week_value(get_row_value(row, ["day_of_week", "weekday"]))
                if day_of_week is not None:
                    updates.append("day_of_week = ?")
                    values.append(day_of_week)

            if has_dom_col:
                day_of_month = parse_int(get_row_value(row, ["day_of_month"]), None, 1, 28)
                if day_of_month is not None:
                    updates.append("day_of_month = ?")
                    values.append(day_of_month)

            values.append(existing["id"])
            cur.execute(f"UPDATE recipients SET {', '.join(updates)} WHERE id = ?", values)
            updated += 1
            continue

        if key in group_name_keys:
            skipped += 1
            skipped_details.append(f"Row {row_no}: name is already used by a group")
            continue

        if key not in latest_keys:
            skipped += 1
            skipped_details.append(f"Row {row_no}: new customer must exist in latest invoice file")
            continue

        frequency, day_of_week, day_of_month = default_schedule_for_terms(terms_code)
        if has_frequency_col:
            parsed_frequency = normalize_frequency(get_row_value(row, ["frequency"]))
            if parsed_frequency:
                frequency = parsed_frequency
        if has_dow_col:
            parsed_dow = parse_day_of_week_value(get_row_value(row, ["day_of_week", "weekday"]))
            if parsed_dow is not None:
                day_of_week = parsed_dow
        if has_dom_col:
            parsed_dom = parse_int(get_row_value(row, ["day_of_month"]), None, 1, 28)
            if parsed_dom is not None:
                day_of_month = parsed_dom

        email_to = ""
        if has_email_col:
            email_to = normalize_email_value(
                get_row_value(row, ["email_to", "email", "emails", "email_address"])
            )

        cur.execute(
            "INSERT INTO recipients(group_name, recipient_type, email_to, net_terms, terms_code, frequency, day_of_week, day_of_month, active, created_at) "
            "VALUES (?, 'single', ?, ?, ?, ?, ?, ?, 1, ?)",
            (customer_name, email_to, net_terms, terms_code, frequency, day_of_week, day_of_month, now),
        )
        new_id = cur.lastrowid
        single_by_key[key] = {"id": new_id, "group_name": customer_name}
        allowed_keys.add(key)
        added += 1

    conn.commit()
    conn.close()
    return added, updated, skipped, skipped_details


def build_excel_template(columns, sheet_name):
    output = BytesIO()
    df = pd.DataFrame(columns=columns)
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    output.seek(0)
    return output


# --- PDF Generation ---

HEADERS = ["Invoice #", "Ship Date", "Due Date", "Total", "Paid Amount", "Status"]
WIDTHS = [35, 30, 30, 35, 35, 25]


def clean_text(text):
    if not isinstance(text, str):
        return str(text)
    replacements = {
        "\u2019": "'",
        "\u2018": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
    }
    for search, replace in replacements.items():
        text = text.replace(search, replace)
    return text.encode("latin-1", "ignore").decode("latin-1")


class PDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_xy(10, 10)
            self.set_font("Arial", "B", 10)
            self.set_text_color(50, 50, 50)
            self.cell(0, 5, get_setting("company_name", ""), 0, 0, "L")
            self.set_xy(10, 15)
            self.set_font("Arial", "", 8)
            self.cell(0, 5, get_setting("company_subtitle", "Statement of Outstanding Invoices"), 0, 0, "L")
            self.set_xy(self.w - 30, 10)
            self.cell(0, 5, f"Page {self.page_no()}/{{nb}}", 0, 0, "R")
            self.set_draw_color(200, 200, 200)
            self.line(10, 22, self.w - 10, 22)
            self.ln(15)


def generate_invoice_pdf(customer_data, output_path, terms_code):
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.add_page()

    logo_path = get_setting("logo_path", "")
    if logo_path and os.path.exists(logo_path):
        try:
            pdf.image(logo_path, x=160, y=10, w=35)
        except Exception:
            pass

    company_name = get_setting("company_name", "REDWAY GROUP INC")
    company_subtitle = get_setting("company_subtitle", "Statement of Outstanding Invoices")
    company_address = get_setting("company_address", "")
    company_phone = get_setting("company_phone", "")
    company_email = get_setting("company_email", "")

    pdf.set_xy(10, 10)
    pdf.set_text_color(44, 62, 80)
    pdf.set_font("Arial", "B", 16)
    pdf.cell(100, 8, txt=company_name, ln=True)
    pdf.set_font("Arial", "B", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(100, 6, txt=company_subtitle, ln=True)
    pdf.set_font("Arial", size=9)
    if company_address:
        pdf.cell(100, 5, txt=company_address, ln=True)
    if company_phone:
        pdf.cell(100, 5, txt=f"Phone: {company_phone}", ln=True)
    if company_email:
        pdf.cell(100, 5, txt=f"Email: {company_email}", ln=True)
    pdf.ln(12)

    try:
        cust_group_name = clean_text(customer_data["Customer Group"].iloc[0])
    except Exception:
        cust_group_name = "Unknown"

    pdf.set_text_color(44, 62, 80)
    pdf.set_font("Arial", "B", 11)
    pdf.cell(40, 8, txt="Group Account:", ln=0)
    pdf.set_font("Arial", "", 11)
    pdf.cell(0, 8, txt=cust_group_name, ln=True)
    pdf.set_font("Arial", "B", 11)
    pdf.cell(40, 8, txt="Terms:", ln=0)
    pdf.set_font("Arial", "", 11)
    terms_label = TERM_CODE_TO_LABEL.get(terms_code, terms_code)
    pdf.cell(0, 8, txt=terms_label, ln=True)
    pdf.set_font("Arial", "B", 11)
    pdf.cell(40, 8, txt="Statement Date:", ln=0)
    pdf.set_font("Arial", "", 11)
    pdf.cell(0, 8, txt=datetime.today().strftime("%B %d, %Y"), ln=True)
    pdf.ln(5)

    customer_data = customer_data.copy()
    processed_list = []
    today = datetime.today().date()

    for _, row in customer_data.iterrows():
        val_total = pd.to_numeric(row.get("Order Total"), errors="coerce")
        val_paid = pd.to_numeric(row.get("Paid Amount", 0), errors="coerce")
        amt = 0.0 if pd.isna(val_total) else float(val_total)
        paid = 0.0 if pd.isna(val_paid) else float(val_paid)

        if amt > 0 and amt > (paid + 0.01):
            ship_date = parse_ship_date(row.get("Shipping Date"))
            row_data = row.copy()
            row_data["C_Total"] = amt
            row_data["C_Paid"] = paid
            row_data["C_ShipDate"] = ship_date
            processed_list.append(row_data)

    if not processed_list:
        return False

    if terms_code == "bill_to_bill":
        sorted_rows = sorted(processed_list, key=lambda r: r["C_ShipDate"])
        for idx, row_data in enumerate(sorted_rows):
            ship_date = row_data["C_ShipDate"]
            if idx < len(sorted_rows) - 1:
                due_date = sorted_rows[idx + 1]["C_ShipDate"]
            else:
                due_date = ship_date + timedelta(days=15)
            row_data["C_DueDate"] = due_date
            row_data["C_Status"] = compute_status(today, due_date)
    else:
        for row_data in processed_list:
            ship_date = row_data["C_ShipDate"]
            due_date = compute_due_date(ship_date, terms_code)
            row_data["C_DueDate"] = due_date
            row_data["C_Status"] = compute_status(today, due_date)

    clean_df = pd.DataFrame(processed_list)
    loc_groups = clean_df.groupby("Location") if "Location" in clean_df.columns else [("Main", clean_df)]

    location_summary = {}
    for location, location_group in loc_groups:
        loc_total_due = 0.0
        loc_total_due_this_week = 0.0

        pdf.set_text_color(44, 62, 80)
        pdf.set_font("Arial", "B", 12)
        pdf.cell(0, 10, txt=f"Location: {clean_text(str(location))}", ln=True)

        pdf.set_font("Arial", "B", 9)
        pdf.set_fill_color(44, 62, 80)
        pdf.set_text_color(255, 255, 255)
        for i in range(len(HEADERS)):
            pdf.cell(WIDTHS[i], 10, HEADERS[i], border="TB", align="C", fill=True)
        pdf.ln()

        pdf.set_font("Arial", size=9)
        pdf.set_draw_color(140, 140, 140)
        pdf.set_line_width(0.3)
        row_height = 8
        for i, row in location_group.reset_index().iterrows():
            status, amt, paid = row["C_Status"], row["C_Total"], row["C_Paid"]
            outstanding = amt - paid

            loc_total_due += outstanding
            if status in ["Overdue", "Due This Week"]:
                loc_total_due_this_week += outstanding

            if status == "Overdue":
                pdf.set_fill_color(255, 230, 230)
            elif status == "Due This Week":
                pdf.set_fill_color(255, 250, 204)
            else:
                pdf.set_fill_color(255, 255, 255) if i % 2 == 0 else pdf.set_fill_color(248, 249, 250)

            pdf.set_text_color(50, 50, 50)
            ship_date = row.get("C_ShipDate")
            due_date = row.get("C_DueDate")
            ship_fmt = ship_date.strftime("%m/%d/%Y") if ship_date else ""
            due_fmt = due_date.strftime("%m/%d/%Y") if due_date else ""

            try:
                inv_no = str(int(float(row.get("Order ID"))))
            except Exception:
                inv_no = str(row.get("Order ID"))

            vals = [clean_text(inv_no), ship_fmt, due_fmt, f"${amt:,.2f}", f"${paid:,.2f}", status]
            for j in range(len(vals)):
                pdf.cell(WIDTHS[j], row_height, vals[j], border=0, align="C", fill=True)
            pdf.ln(row_height)
            y = pdf.get_y()
            x = pdf.l_margin
            pdf.line(x, y, x + sum(WIDTHS), y)

        location_summary[location] = {
            "outstanding": loc_total_due,
            "due_this_week": loc_total_due_this_week,
        }
        pdf.ln(10)

    pdf.set_draw_color(44, 62, 80)
    pdf.set_line_width(0.5)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(5)

    pdf.set_font("Arial", "B", 12)
    pdf.set_text_color(44, 62, 80)
    pdf.cell(0, 10, "ACCOUNT SUMMARY BY LOCATION", ln=True)

    for loc, totals in location_summary.items():
        pdf.set_text_color(50, 50, 50)
        pdf.set_font("Arial", "B", 10)
        pdf.cell(80, 7, f"{clean_text(str(loc))}:", 0, 0)

        pdf.set_font("Arial", "", 10)
        pdf.cell(50, 7, f"Outstanding: ${totals['outstanding']:,.2f}", 0, 0, "R")
        pdf.set_font("Arial", "B", 10)
        pdf.cell(60, 7, f"Due This Week: ${totals['due_this_week']:,.2f}", 0, 1, "R")

    pdf.output(output_path)
    return True


# --- Email ---

def send_email(to_emails, subject, body, attachment_path=None, cc_emails=None, extra_attachments=None):
    to_emails = normalize_email_value(to_emails)
    if not to_emails:
        raise RuntimeError("Missing recipient email")

    host = get_setting("smtp_host")
    port = int(get_setting("smtp_port", "587") or "587")
    username = get_setting("smtp_user")
    password = get_setting("smtp_pass")
    sender = get_setting("smtp_from", username)
    use_tls = get_setting("smtp_tls", "true").lower() == "true"
    logo_path = get_setting("logo_path", "")
    try:
        smtp_timeout = float(get_setting("smtp_timeout", "20") or "20")
    except Exception:
        smtp_timeout = 20.0

    if not host or not sender:
        raise RuntimeError("SMTP settings are incomplete")

    logo_cid = None
    logo_html = ""
    if logo_path and os.path.exists(logo_path):
        logo_cid = make_msgid()[1:-1]
        logo_html = f'<img width="96" height="96" src="cid:{logo_cid}" style="display:block" alt="Logo" />'

    signature_text, signature_html = build_signature(logo_html)
    plain_body = f"{body}\n\n{signature_text}".strip()

    html_body = body.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    html_body = html_body.replace("\n", "<br>")
    html_body = f"{html_body}<br><br>{signature_html}"

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = to_emails
    if cc_emails:
        msg["Cc"] = cc_emails
    msg["Subject"] = subject
    msg.set_content(plain_body)
    msg.add_alternative(html_body, subtype="html")

    if logo_cid and logo_path and os.path.exists(logo_path):
        ctype, _ = mimetypes.guess_type(logo_path)
        maintype, subtype = ("image", "png")
        if ctype and "/" in ctype:
            maintype, subtype = ctype.split("/", 1)
        with open(logo_path, "rb") as f:
            logo_data = f.read()
        html_part = msg.get_payload()[1]
        html_part.add_related(
            logo_data,
            maintype=maintype,
            subtype=subtype,
            cid=logo_cid,
        )

    attachments = []
    if attachment_path:
        with open(attachment_path, "rb") as f:
            data = f.read()
        filename = os.path.basename(attachment_path)
        attachments.append(
            {
                "data": data,
                "filename": filename,
                "content_type": "application/pdf",
            }
        )

    if extra_attachments:
        attachments.extend(extra_attachments)

    for attachment in attachments:
        ctype = attachment.get("content_type") or "application/octet-stream"
        maintype, subtype = ("application", "octet-stream")
        if "/" in ctype:
            maintype, subtype = ctype.split("/", 1)
        msg.add_attachment(
            attachment["data"],
            maintype=maintype,
            subtype=subtype,
            filename=attachment.get("filename", "attachment"),
        )

    with smtplib.SMTP(host, port, timeout=smtp_timeout) as server:
        if use_tls:
            server.starttls()
        if username and password:
            server.login(username, password)
        server.send_message(msg)


# --- Scheduling helpers ---

def parse_date(value):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except Exception:
        return None


def is_due(recipient, today):
    if not recipient["active"]:
        return False
    frequency = recipient["frequency"]
    if frequency == "none":
        return False

    last_sent = parse_date(recipient["last_sent"])
    day_of_week = int(recipient["day_of_week"])
    day_of_month = int(recipient["day_of_month"])

    if frequency == "weekly":
        if today.weekday() != day_of_week:
            return False
        if not last_sent:
            return True
        return (today - last_sent).days >= 7

    if frequency == "biweekly":
        if today.weekday() != day_of_week:
            return False
        if not last_sent:
            return True
        return (today - last_sent).days >= 14

    if frequency == "monthly":
        if today.day != day_of_month:
            return False
        if not last_sent:
            return True
        return last_sent.month != today.month or last_sent.year != today.year

    return False


SCHEDULE_WORKER_LOCK = threading.Lock()
SCHEDULE_WORKER_THREAD = None


def now_ts():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def parse_json_list(value):
    if not value:
        return []
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return parsed
    except Exception:
        pass
    return []


def get_setting_float(key, default=0.0, min_value=None, max_value=None):
    raw = get_setting(key, str(default))
    try:
        value = float(raw)
    except Exception:
        value = float(default)
    if min_value is not None and value < min_value:
        value = min_value
    if max_value is not None and value > max_value:
        value = max_value
    return value


def get_due_recipients(today=None):
    if today is None:
        today = date.today()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM recipients ORDER BY group_name ASC")
    rows = cur.fetchall()
    conn.close()

    grouped_customer_ids = get_grouped_customer_ids()
    due = []
    for row in rows:
        if row["recipient_type"] == "single" and row["id"] in grouped_customer_ids:
            continue
        if is_due(row, today):
            due.append(dict(row))
    return due


def get_active_scheduled_job():
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM scheduled_jobs WHERE status IN ('queued', 'running') ORDER BY created_at DESC LIMIT 1"
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    job = dict(row)
    job["missing_email_customers"] = parse_json_list(job.get("missing_email_customers"))
    return job


def get_recent_scheduled_jobs(limit=10):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM scheduled_jobs ORDER BY created_at DESC LIMIT ?",
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    jobs = []
    for row in rows:
        job = dict(row)
        total = int(job.get("total_items") or 0)
        processed = int(job.get("processed_items") or 0)
        job["progress_pct"] = round((processed / total) * 100, 1) if total > 0 else 0.0
        job["missing_email_customers"] = parse_json_list(job.get("missing_email_customers"))
        jobs.append(job)
    return jobs


def create_scheduled_job(requested_by="system"):
    invoice_file_id, invoice_path = get_invoice_for_run()
    due_recipients = get_due_recipients(date.today())

    max_recipients = parse_int(get_setting("scheduled_max_recipients", "0"), 0, 0)
    if max_recipients > 0:
        due_recipients = due_recipients[:max_recipients]

    if not due_recipients:
        return None, "No statements due today."

    now = now_ts()
    conn = get_db()
    cur = conn.cursor()
    cur.execute("BEGIN IMMEDIATE")
    cur.execute(
        "SELECT id, status FROM scheduled_jobs WHERE status IN ('queued', 'running') ORDER BY created_at DESC LIMIT 1"
    )
    active = cur.fetchone()
    if active:
        conn.rollback()
        conn.close()
        return None, f"Scheduled run already {active['status']} (Job #{active['id']})."

    cur.execute(
        "INSERT INTO scheduled_jobs(status, created_at, last_heartbeat, invoice_file_id, invoice_path, requested_by, total_items) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        ("queued", now, now, invoice_file_id, invoice_path, requested_by or "system", len(due_recipients)),
    )
    job_id = cur.lastrowid
    cur.executemany(
        "INSERT INTO scheduled_job_items(job_id, recipient_id, recipient_name, status, created_at) VALUES (?, ?, ?, ?, ?)",
        [(job_id, r["id"], r["group_name"], "pending", now) for r in due_recipients],
    )
    conn.commit()
    conn.close()
    return job_id, None


def claim_next_scheduled_job():
    now = now_ts()
    stale_seconds = parse_int(get_setting("scheduled_stale_seconds", "900"), 900, 60)
    stale_cutoff = (datetime.now() - timedelta(seconds=stale_seconds)).strftime("%Y-%m-%d %H:%M:%S")

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE scheduled_jobs SET status = 'queued', started_at = NULL "
        "WHERE status = 'running' AND (last_heartbeat IS NULL OR last_heartbeat < ?)",
        (stale_cutoff,),
    )
    conn.commit()

    cur.execute(
        "SELECT id FROM scheduled_jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1"
    )
    row = cur.fetchone()
    if not row:
        conn.close()
        return None

    job_id = int(row["id"])
    cur.execute(
        "UPDATE scheduled_jobs SET status = 'running', started_at = COALESCE(started_at, ?), last_heartbeat = ? "
        "WHERE id = ? AND status = 'queued'",
        (now, now, job_id),
    )
    claimed = cur.rowcount == 1
    conn.commit()
    conn.close()
    return job_id if claimed else None


def mark_scheduled_job_failed(job_id, error_message):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "UPDATE scheduled_jobs SET status = 'failed', finished_at = ?, last_heartbeat = ?, error = ? WHERE id = ?",
        (now_ts(), now_ts(), str(error_message), job_id),
    )
    conn.commit()
    conn.close()


def is_retryable_send_error(message):
    text = str(message or "").lower()
    tokens = [
        "timed out",
        "timeout",
        "temporarily unavailable",
        "temporary",
        "connection unexpectedly closed",
        "connection reset",
        "server disconnected",
        "service not available",
        "try again",
        "smtpserverdisconnected",
        "421",
        "450",
        "451",
        "452",
        "454",
    ]
    return any(token in text for token in tokens)


def process_scheduled_job(job_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM scheduled_jobs WHERE id = ?", (job_id,))
    job = cur.fetchone()
    conn.close()
    if not job:
        return

    invoice_path = job["invoice_path"]
    invoice_file_id = job["invoice_file_id"]
    job_created_at = job["created_at"]
    try:
        invoice_df = load_invoice_df(invoice_path)
    except Exception as exc:
        mark_scheduled_job_failed(job_id, exc)
        return

    send_delay = get_setting_float("scheduled_send_delay_seconds", 1.0, 0.0, 30.0)
    retry_count = parse_int(get_setting("scheduled_send_retries", "2"), 2, 0, 5)
    retry_backoff = get_setting_float("scheduled_retry_backoff_seconds", 3.0, 0.0, 60.0)
    max_attempts = retry_count + 1

    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT * FROM scheduled_job_items WHERE job_id = ? AND status IN ('pending', 'running') ORDER BY id ASC",
        (job_id,),
    )
    items = cur.fetchall()

    processed = int(job["processed_items"] or 0)
    sent = int(job["sent_count"] or 0)
    skipped = int(job["skipped_count"] or 0)
    failed = int(job["failed_count"] or 0)
    missing_names = set(parse_json_list(job["missing_email_customers"]))

    for idx, item in enumerate(items):
        recipient_id = int(item["recipient_id"])
        recipient_name = item["recipient_name"]
        attempts = int(item["attempts"] or 0)
        final_status = "failed"
        detail = "Unknown failure"

        while attempts < max_attempts:
            attempts += 1
            now = now_ts()
            cur.execute(
                "UPDATE scheduled_job_items SET status = 'running', started_at = COALESCE(started_at, ?), attempts = ? WHERE id = ?",
                (now, attempts, item["id"]),
            )
            cur.execute(
                "UPDATE scheduled_jobs SET last_heartbeat = ? WHERE id = ?",
                (now, job_id),
            )
            conn.commit()

            cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
            recipient = cur.fetchone()
            if not recipient:
                final_status = "failed"
                detail = "Recipient not found"
                break

            if invoice_file_id is None:
                cur.execute(
                    "SELECT id FROM statement_runs WHERE recipient_id = ? AND run_type = 'scheduled' "
                    "AND status = 'sent' AND created_at >= ? AND invoice_file_id IS NULL "
                    "ORDER BY id DESC LIMIT 1",
                    (recipient_id, job_created_at),
                )
            else:
                cur.execute(
                    "SELECT id FROM statement_runs WHERE recipient_id = ? AND run_type = 'scheduled' "
                    "AND status = 'sent' AND created_at >= ? AND invoice_file_id = ? "
                    "ORDER BY id DESC LIMIT 1",
                    (recipient_id, job_created_at, invoice_file_id),
                )
            already_sent = cur.fetchone()
            if already_sent:
                final_status = "sent"
                detail = "Recovered previous sent state"
                break

            status, detail = run_for_recipient(
                dict(recipient),
                invoice_path,
                invoice_file_id,
                "scheduled",
                preloaded_df=invoice_df,
            )
            if status == "error" and attempts < max_attempts and is_retryable_send_error(detail):
                if retry_backoff > 0:
                    time.sleep(retry_backoff * attempts)
                continue
            final_status = "failed" if status == "error" else status
            break

        if final_status == "sent":
            sent += 1
        elif final_status == "skipped":
            skipped += 1
            if detail == "Missing recipient email":
                missing_names.add(recipient_name)
        else:
            failed += 1
        processed += 1

        cur.execute(
            "UPDATE scheduled_job_items SET status = ?, error = ?, attempts = ?, finished_at = ? WHERE id = ?",
            (final_status, str(detail), attempts, now_ts(), item["id"]),
        )
        cur.execute(
            "UPDATE scheduled_jobs SET processed_items = ?, sent_count = ?, skipped_count = ?, failed_count = ?, "
            "last_heartbeat = ?, missing_email_customers = ? WHERE id = ?",
            (
                processed,
                sent,
                skipped,
                failed,
                now_ts(),
                json.dumps(sorted(missing_names)),
                job_id,
            ),
        )
        conn.commit()

        if send_delay > 0 and idx < len(items) - 1:
            time.sleep(send_delay)

    summary_error = ""
    if failed > 0:
        cur.execute(
            "SELECT error FROM scheduled_job_items "
            "WHERE job_id = ? AND status = 'failed' AND error IS NOT NULL AND TRIM(error) <> '' "
            "ORDER BY id DESC LIMIT 3",
            (job_id,),
        )
        samples = [row["error"] for row in cur.fetchall() if row["error"]]
        if samples:
            summary_error = "; ".join(samples)
        else:
            summary_error = f"{failed} recipient(s) failed."

    cur.execute(
        "UPDATE scheduled_jobs SET status = 'completed', finished_at = ?, last_heartbeat = ?, error = ?, "
        "missing_email_customers = ? WHERE id = ?",
        (now_ts(), now_ts(), summary_error, json.dumps(sorted(missing_names)), job_id),
    )
    conn.commit()
    conn.close()


def scheduled_worker_loop():
    while True:
        try:
            job_id = claim_next_scheduled_job()
            if not job_id:
                time.sleep(2.0)
                continue
            process_scheduled_job(job_id)
        except Exception as exc:
            app.logger.exception("Scheduled worker failed: %s", exc)
            time.sleep(2.0)


def ensure_schedule_worker_running():
    global SCHEDULE_WORKER_THREAD
    with SCHEDULE_WORKER_LOCK:
        if SCHEDULE_WORKER_THREAD and SCHEDULE_WORKER_THREAD.is_alive():
            return
        SCHEDULE_WORKER_THREAD = threading.Thread(
            target=scheduled_worker_loop,
            name="schedule-worker",
            daemon=True,
        )
        SCHEDULE_WORKER_THREAD.start()


def load_invoice_df(invoice_path):
    df = pd.read_excel(invoice_path)
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise RuntimeError(f"Invoice file missing columns: {', '.join(sorted(missing))}")
    return df


def apply_mappings(df):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT m.customer_name, r.id, r.group_name, r.net_terms, r.location, r.terms_code "
        "FROM customer_mappings m JOIN recipients r ON r.id = m.recipient_id"
    )
    mapping_rows = cur.fetchall()
    conn.close()

    mapping = {
        row[0]: {
            "recipient_id": row[1],
            "group_name": row[2],
            "net_terms": row[3],
            "location": row[4],
            "terms_code": row[5],
        }
        for row in mapping_rows
    }

    def map_group(name):
        entry = mapping.get(name)
        return entry["group_name"] if entry else None

    df = df.copy()
    if "Customer Group" not in df.columns:
        df["Customer Group"] = df["Customer Name"].map(map_group)
    else:
        missing_mask = df["Customer Group"].isna() | (df["Customer Group"].astype(str).str.strip() == "")
        df.loc[missing_mask, "Customer Group"] = df.loc[missing_mask, "Customer Name"].map(map_group)

    if "Location" not in df.columns:
        df["Location"] = df["Customer Name"]

    return df, mapping


def get_latest_invoice_file():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, path FROM invoice_files ORDER BY uploaded_at DESC LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row if row else None


def get_latest_invoice_customer_names():
    latest = get_latest_invoice_file()
    if not latest:
        return [], None

    latest_path = latest["path"]
    invoice_label = os.path.basename(latest_path)
    df = load_invoice_df(latest_path)
    names = [normalize_name(name) for name in df["Customer Name"].tolist()]
    unique_names = sorted({name for name in names if name})
    return unique_names, invoice_label


def get_invoice_for_run():
    source = get_setting("invoice_source", "latest_upload")
    if source == "path":
        path = get_setting("invoice_path")
        if not path or not os.path.exists(path):
            raise RuntimeError("Invoice path is not set or does not exist")
        return None, path

    latest = get_latest_invoice_file()
    if not latest:
        raise RuntimeError("No uploaded invoice file found")
    return latest[0], latest[1]


def get_group_member_names(group_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT r.group_name FROM group_members gm "
        "JOIN recipients r ON r.id = gm.customer_id WHERE gm.group_id = ? AND r.active = 1",
        (group_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return [row["group_name"] for row in rows]


def get_group_member_records(group_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute(
        "SELECT r.id, r.group_name FROM group_members gm "
        "JOIN recipients r ON r.id = gm.customer_id "
        "WHERE gm.group_id = ? AND r.active = 1",
        (group_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def build_recipient_df(recipient, df):
    df = df.copy()
    df["_customer_key"] = df["Customer Name"].apply(name_key)

    if recipient["recipient_type"] == "group":
        members = get_group_member_records(recipient["id"])
        member_ids = [row["id"] for row in members]
        aliases_by_id = get_alias_names_by_recipient_ids(member_ids)
        member_keys = set()
        for member in members:
            base_key = name_key(member["group_name"])
            if base_key:
                member_keys.add(base_key)
            for alias_name in aliases_by_id.get(member["id"], []):
                alias_key = name_key(alias_name)
                if alias_key:
                    member_keys.add(alias_key)
        if not member_keys:
            raise RuntimeError("Group has no members")
        customer_df = df[df["_customer_key"].isin(member_keys)].copy()
        if customer_df.empty:
            raise RuntimeError("No invoice rows matched this recipient")
        customer_df["Location"] = customer_df["Customer Name"]
    else:
        recipient_keys = set()
        base_key = name_key(recipient["group_name"])
        if base_key:
            recipient_keys.add(base_key)
        aliases_by_id = get_alias_names_by_recipient_ids([recipient["id"]])
        for alias_name in aliases_by_id.get(recipient["id"], []):
            alias_key = name_key(alias_name)
            if alias_key:
                recipient_keys.add(alias_key)
        customer_df = df[df["_customer_key"].isin(recipient_keys)].copy()
        if customer_df.empty:
            raise RuntimeError("No invoice rows matched this recipient")
        # Singles (including merged aliases) should render as a single location block.
        customer_df["Location"] = recipient["group_name"]

    customer_df["Customer Group"] = recipient["group_name"]
    customer_df = customer_df.drop(columns=["_customer_key"])
    return customer_df


def run_for_recipient(recipient, invoice_path, invoice_file_id, run_type, preloaded_df=None):
    run_id = None
    conn = get_db()
    cur = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "INSERT INTO statement_runs(recipient_id, invoice_file_id, run_type, status, created_at) VALUES (?, ?, ?, ?, ?)",
        (recipient["id"], invoice_file_id, run_type, "started", now),
    )
    run_id = cur.lastrowid
    conn.commit()
    conn.close()

    try:
        df = preloaded_df if preloaded_df is not None else load_invoice_df(invoice_path)
        customer_df = build_recipient_df(recipient, df)

        os.makedirs(OUT_DIR, exist_ok=True)
        safe_name = "".join([c for c in recipient["group_name"] if c.isalnum() or c in (" ", "-", "_")]).strip()
        filename = f"{safe_name}_Statement_{date.today().strftime('%Y%m%d')}.pdf"
        output_path = os.path.join(OUT_DIR, filename)

        terms_code = recipient["terms_code"] or normalize_terms_code(recipient["net_terms"]) or "net_30"
        ok = generate_invoice_pdf(customer_df, output_path, terms_code)
        if not ok:
            raise RuntimeError("No outstanding invoices to include")

        subject = f"Statement of Open Invoices {date.today().strftime('%m/%d/%Y')}"
        body = get_email_template_body("statement")
        recipient_email = normalize_email_value(recipient["email_to"])
        if not recipient_email:
            raise RuntimeError("Missing recipient email")
        send_email(recipient_email, subject, body, output_path, cc_emails=get_notice_cc("statement"))

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "UPDATE statement_runs SET status = ?, sent_at = ?, pdf_path = ? WHERE id = ?",
            ("sent", now, output_path, run_id),
        )
        cur.execute(
            "UPDATE recipients SET last_sent = ? WHERE id = ?",
            (date.today().strftime("%Y-%m-%d"), recipient["id"]),
        )
        conn.commit()
        conn.close()

        return "sent", output_path
    except Exception as exc:
        conn = get_db()
        cur = conn.cursor()
        message = str(exc)
        status = "error"
        if run_type == "scheduled" and message in {
            "No outstanding invoices to include",
            "No invoice rows matched this recipient",
            "Group has no members",
            "Missing recipient email",
        }:
            status = "skipped"
        cur.execute(
            "UPDATE statement_runs SET status = ?, error = ? WHERE id = ?",
            (status, message, run_id),
        )
        conn.commit()
        conn.close()
        if run_type == "scheduled":
            return status, message
        raise


def build_statement_pdf(recipient, invoice_path):
    df = load_invoice_df(invoice_path)
    customer_df = build_recipient_df(recipient, df)

    os.makedirs(OUT_DIR, exist_ok=True)
    safe_name = "".join([c for c in recipient["group_name"] if c.isalnum() or c in (" ", "-", "_")]).strip()
    filename = f"{safe_name}_Statement_{date.today().strftime('%Y%m%d')}.pdf"
    output_path = os.path.join(OUT_DIR, filename)

    terms_code = recipient["terms_code"] or normalize_terms_code(recipient["net_terms"]) or "net_30"
    ok = generate_invoice_pdf(customer_df, output_path, terms_code)
    if not ok:
        raise RuntimeError("No outstanding invoices to include")

    return output_path


# --- Routes ---

@app.route("/login", methods=["GET", "POST"])
def login():
    if not auth_enabled():
        return redirect(url_for("index"))

    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "").strip()

        if APP_USERNAME and username.lower() != APP_USERNAME.lower():
            flash("Invalid credentials.", "error")
            return redirect(url_for("login"))

        if password != APP_PASSWORD:
            flash("Invalid credentials.", "error")
            return redirect(url_for("login"))

        session["logged_in"] = True
        session["username"] = username or APP_USERNAME or "user"
        next_url = request.args.get("next")
        if not next_url or not next_url.startswith("/"):
            next_url = url_for("index")
        return redirect(next_url)

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


@app.route("/assets/logo")
def app_logo():
    logo_path = get_setting("logo_path", "")
    if logo_path and os.path.exists(logo_path):
        return send_file(logo_path)
    return "", 404


@app.context_processor
def inject_app_logo():
    logo_path = get_setting("logo_path", "")
    has_logo = bool(logo_path and os.path.exists(logo_path))
    return {"app_logo_url": url_for("app_logo") if has_logo else ""}


@app.route("/")
def index():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM recipients WHERE recipient_type = 'single'")
    customer_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM recipients WHERE recipient_type = 'group'")
    group_count = cur.fetchone()[0]
    cutoff = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "SELECT sr.*, r.group_name FROM statement_runs sr "
        "JOIN recipients r ON r.id = sr.recipient_id "
        "WHERE sr.created_at >= ? "
        "ORDER BY sr.created_at DESC LIMIT 30",
        (cutoff,),
    )
    runs = cur.fetchall()
    cur.execute("SELECT * FROM recipients ORDER BY group_name ASC")
    recipients = cur.fetchall()
    conn.close()
    active_schedule_job = get_active_scheduled_job()
    schedule_jobs = get_recent_scheduled_jobs(limit=20)

    today = date.today()
    grouped_customer_ids = get_grouped_customer_ids()
    due = [
        r
        for r in recipients
        if is_due(r, today)
        and not (r["recipient_type"] == "single" and r["id"] in grouped_customer_ids)
    ]

    financials = compute_dashboard_financials()
    overdue_chart = build_pie_chart(
        [
            ("Overdue", financials["overdue_amount"], "#d14f4f"),
            ("Current", financials["current_amount"], "#2d8a4e"),
        ]
    )

    terms_counts, terms_customers = get_terms_distribution(include_customers=True)
    term_color_map = {
        "net_7": "#1f77b4",
        "net_15": "#ff7f0e",
        "net_20": "#2ca02c",
        "net_30": "#d62728",
        "net_45": "#9467bd",
        "cod": "#8c564b",
        "bill_to_bill": "#e377c2",
        "month_to_month": "#7f7f7f",
        "week_to_week": "#bcbd22",
    }
    terms_segments = []
    known_codes = [code for code, _ in TERM_OPTIONS]
    for code in known_codes:
        count = terms_counts.get(code, 0)
        if count > 0:
            terms_segments.append(
                (
                    code,
                    TERM_CODE_TO_LABEL.get(code, code),
                    float(count),
                    term_color_map.get(code, "#17becf"),
                )
            )
    for code, count in terms_counts.items():
        if code in known_codes or count <= 0:
            continue
        terms_segments.append((code, TERM_CODE_TO_LABEL.get(code, code), float(count), "#17becf"))
    terms_chart = build_treemap_chart(terms_segments)

    return render_template(
        "index.html",
        customer_count=customer_count,
        group_count=group_count,
        runs=runs,
        due=due,
        today=today,
        total_receivable=financials["total_receivable"],
        total_overdue_amount=financials["overdue_amount"],
        total_current_amount=financials["current_amount"],
        dashboard_invoice_label=financials["invoice_label"],
        dashboard_error=financials["error"],
        overdue_chart=overdue_chart,
        terms_chart=terms_chart,
        terms_customers=terms_customers,
        active_schedule_job=active_schedule_job,
        schedule_jobs=schedule_jobs,
    )


@app.route("/overdue-report")
def overdue_report():
    today = date.today()
    run = get_latest_overdue_run()

    rows = get_overdue_items(run["id"]) if run and run["status"] == "success" else []
    rows = [dict(row) for row in rows]
    invoice_file_id = run["invoice_file_id"] if run else None
    invoice_path = run["invoice_path"] if run else None
    sent_map = get_notice_sends(invoice_file_id, invoice_path) if run else set()
    recipients_map = get_recipients_terms_map()
    for row in rows:
        recipient = recipients_map.get(row["group_name"])
        row["recipient_id"] = recipient["id"] if recipient else None
        row["has_email"] = bool(recipient and recipient.get("has_email"))
        skipped_raw = row.get("skipped_invoices") or "[]"
        try:
            row["skipped_invoices"] = json.loads(skipped_raw)
        except Exception:
            row["skipped_invoices"] = []
        row["skipped_count"] = row.get("skipped_count") or len(row["skipped_invoices"])
        row["short_paid_count"] = row.get("short_paid_count") or 0
        row["short_paid_amount"] = row.get("short_paid_amount") or 0.0
        short_paid_raw = row.get("short_paid_invoices") or "[]"
        try:
            row["short_paid_invoices"] = json.loads(short_paid_raw)
        except Exception:
            row["short_paid_invoices"] = []
        recipient_id = row["recipient_id"]
        row["sent_overdue"] = (recipient_id, "overdue") in sent_map if recipient_id else False
        row["sent_skipped"] = (recipient_id, "skipped") in sent_map if recipient_id else False
        row["sent_short_paid"] = (recipient_id, "short_paid") in sent_map if recipient_id else False

    total_overdue_count = sum(row["overdue_count"] for row in rows)
    total_overdue_amount = sum(row["overdue_amount"] for row in rows)
    total_short_paid_count = sum(row.get("short_paid_count", 0) for row in rows)
    total_short_paid_amount = sum(row.get("short_paid_amount", 0.0) for row in rows)
    invoice_label = run["filename"] if run else None
    if not invoice_label and run and run["invoice_path"]:
        invoice_label = os.path.basename(run["invoice_path"])

    term_labels = {code: label for code, label in TERM_OPTIONS}
    return render_template(
        "overdue_report.html",
        rows=rows,
        run=run,
        invoice_label=invoice_label,
        today=today,
        term_labels=term_labels,
        total_overdue_count=total_overdue_count,
        total_overdue_amount=total_overdue_amount,
        total_short_paid_count=total_short_paid_count,
        total_short_paid_amount=total_short_paid_amount,
    )


@app.route("/overdue-report/run", methods=["POST"])
def overdue_report_run():
    status, error = run_overdue_report()
    if status == "success":
        flash("Overdue report generated.", "success")
    else:
        flash(f"Overdue report failed: {error}", "error")
    return redirect(url_for("overdue_report"))


@app.route("/overdue-report/export")
def overdue_report_export():
    run = get_latest_overdue_run()

    if not run or run["status"] != "success":
        return "No overdue report available to export.", 400

    rows = get_overdue_items(run["id"])
    if not rows:
        return "No overdue data to export.", 400

    data = []
    for row in rows:
        terms_label = TERM_CODE_TO_LABEL.get(row["terms_code"], row["terms_code"])
        data.append(
            {
                "Group": row["group_name"],
                "Terms": terms_label,
                "Overdue Invoices": int(row["overdue_count"]),
                "Oldest Overdue Days": int(row["days_overdue"]),
                "Overdue Amount": float(row["overdue_amount"]),
                "Short Paid Invoices": int(row.get("short_paid_count", 0)),
                "Short Paid Amount": float(row.get("short_paid_amount", 0.0)),
            }
        )

    df = pd.DataFrame(data)
    output = BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="overdue_report")
    output.seek(0)

    filename = f"overdue_report_{date.today().strftime('%Y%m%d')}.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/overdue-report/send/<int:recipient_id>", methods=["POST"])
def overdue_report_send(recipient_id):
    run_id = resolve_run_id(request.form.get("run_id"))
    invoice_path = None
    invoice_file_id = None
    if run_id:
        run = get_overdue_run(int(run_id))
        if run and run["invoice_path"]:
            invoice_path = run["invoice_path"]
        if run:
            invoice_file_id = run["invoice_file_id"]

    try:
        if not invoice_path:
            _, invoice_path = get_invoice_for_run()

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
        recipient = cur.fetchone()
        conn.close()
        if not recipient:
            flash("Recipient not found", "error")
            return redirect(url_for("overdue_report"))
        recipient = dict(recipient)
        recipient = ensure_recipient_email(recipient, request.form.get("email_to", ""))

        output_path = build_statement_pdf(recipient, invoice_path)
        subject = f"Overdue Notice {date.today().strftime('%m/%d/%Y')}"
        body = get_email_template_body("overdue")
        send_email(recipient["email_to"], subject, body, output_path, cc_emails=get_notice_cc("overdue"))
        record_notice_send(run_id, invoice_file_id, invoice_path, recipient_id, "overdue")
        flash(f"Overdue notice sent to {recipient['group_name']}.", "success")
    except Exception as exc:
        flash(f"Overdue notice failed: {exc}", "error")

    return redirect(url_for("overdue_report"))


@app.route("/overdue-report/skipped/<int:recipient_id>", methods=["POST"])
def overdue_report_skipped(recipient_id):
    run_id = resolve_run_id(request.form.get("run_id"))
    invoice_ids = request.form.getlist("invoice_ids")
    uploaded_files = request.files.getlist("invoice_files")

    if not invoice_ids:
        flash("No skipped invoices provided.", "error")
        return redirect(url_for("overdue_report"))
    if not uploaded_files or len(uploaded_files) != len(invoice_ids):
        flash("Please upload one PDF for each skipped invoice.", "error")
        return redirect(url_for("overdue_report"))

    invoice_path = None
    invoice_file_id = None
    if run_id:
        run = get_overdue_run(int(run_id))
        if run and run["invoice_path"]:
            invoice_path = run["invoice_path"]
        if run:
            invoice_file_id = run["invoice_file_id"]

    try:
        if not invoice_path:
            _, invoice_path = get_invoice_for_run()

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
        recipient = cur.fetchone()
        conn.close()
        if not recipient:
            flash("Recipient not found", "error")
            return redirect(url_for("overdue_report"))
        recipient = dict(recipient)
        recipient = ensure_recipient_email(recipient, request.form.get("email_to", ""))

        attachments = []
        for idx, file in enumerate(uploaded_files):
            if not file or not file.filename:
                flash("Each skipped invoice must have a PDF attached.", "error")
                return redirect(url_for("overdue_report"))
            data = file.read()
            if not data:
                flash("One or more uploaded files were empty.", "error")
                return redirect(url_for("overdue_report"))
            filename = file.filename or f"invoice_{invoice_ids[idx]}.pdf"
            attachments.append(
                {
                    "data": data,
                    "filename": filename,
                    "content_type": file.mimetype or "application/pdf",
                }
            )

        statement_path = build_statement_pdf(recipient, invoice_path)
        subject = f"Skipped Invoice Notifcation {date.today().strftime('%m/%d/%Y')}"
        body = get_email_template_body("skipped")
        send_email(
            recipient["email_to"],
            subject,
            body,
            attachment_path=statement_path,
            cc_emails=get_notice_cc("skipped"),
            extra_attachments=attachments,
        )
        record_notice_send(run_id, invoice_file_id, invoice_path, recipient_id, "skipped")
        flash(f"Skipped notice sent to {recipient['group_name']}.", "success")
    except Exception as exc:
        flash(f"Skipped notice failed: {exc}", "error")

    return redirect(url_for("overdue_report"))


@app.route("/overdue-report/short-paid/<int:recipient_id>", methods=["POST"])
def overdue_report_short_paid(recipient_id):
    run_id = resolve_run_id(request.form.get("run_id"))
    invoice_ids = request.form.getlist("invoice_ids")
    uploaded_files = request.files.getlist("invoice_files")

    if not invoice_ids:
        flash("No short-paid invoices provided.", "error")
        return redirect(url_for("overdue_report"))
    if not uploaded_files or len(uploaded_files) != len(invoice_ids):
        flash("Please upload one PDF for each short-paid invoice.", "error")
        return redirect(url_for("overdue_report"))

    invoice_path = None
    invoice_file_id = None
    if run_id:
        run = get_overdue_run(int(run_id))
        if run and run["invoice_path"]:
            invoice_path = run["invoice_path"]
        if run:
            invoice_file_id = run["invoice_file_id"]

    try:
        if not invoice_path:
            _, invoice_path = get_invoice_for_run()

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
        recipient = cur.fetchone()
        conn.close()
        if not recipient:
            flash("Recipient not found", "error")
            return redirect(url_for("overdue_report"))
        recipient = dict(recipient)
        recipient = ensure_recipient_email(recipient, request.form.get("email_to", ""))

        attachments = []
        for idx, file in enumerate(uploaded_files):
            if not file or not file.filename:
                flash("Each short-paid invoice must have a PDF attached.", "error")
                return redirect(url_for("overdue_report"))
            data = file.read()
            if not data:
                flash("One or more uploaded files were empty.", "error")
                return redirect(url_for("overdue_report"))
            filename = file.filename or f"invoice_{invoice_ids[idx]}.pdf"
            attachments.append(
                {
                    "data": data,
                    "filename": filename,
                    "content_type": file.mimetype or "application/pdf",
                }
            )

        statement_path = build_statement_pdf(recipient, invoice_path)
        subject = f"Partial Payment Notification {date.today().strftime('%m/%d/%Y')}"
        body = get_email_template_body("short_paid")
        send_email(
            recipient["email_to"],
            subject,
            body,
            attachment_path=statement_path,
            cc_emails=get_notice_cc("short_paid"),
            extra_attachments=attachments,
        )
        record_notice_send(run_id, invoice_file_id, invoice_path, recipient_id, "short_paid")
        flash(f"Short paid notice sent to {recipient['group_name']}.", "success")
    except Exception as exc:
        flash(f"Short paid notice failed: {exc}", "error")

    return redirect(url_for("overdue_report"))


@app.route("/customers/<int:recipient_id>/statement")
@app.route("/recipients/<int:recipient_id>/statement")
def download_statement(recipient_id):
    try:
        invoice_file_id, invoice_path = get_invoice_for_run()
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
        recipient = cur.fetchone()
        conn.close()
        if not recipient:
            return "Recipient not found", 404

        output_path = build_statement_pdf(recipient, invoice_path)
        filename = os.path.basename(output_path)
        return send_file(
            output_path,
            mimetype="application/pdf",
            as_attachment=False,
            download_name=filename,
        )
    except Exception as exc:
        return f"Statement download failed: {exc}", 400


@app.route("/customers/bulk-template")
def customers_bulk_template():
    columns = [
        "Customer Name",
        "Terms",
        "Email",
        "Frequency",
        "Day of Week",
        "Day of Month",
    ]
    output = build_excel_template(columns, "bulk_customers")
    filename = "customers_bulk_update_template.xlsx"
    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


@app.route("/customers", methods=["GET", "POST"])
@app.route("/recipients", methods=["GET", "POST"])
def customers():
    if request.method == "POST":
        form_type = request.form.get("form_type", "")
        conn = get_db()
        cur = conn.cursor()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if form_type == "create_single":
            customer_name = request.form.get("customer_name", "").strip()
            email_to = normalize_email_value(request.form.get("email_to", ""))
            terms_code = get_terms_code(request.form.get("terms_code", "net_30"))
            net_terms = get_terms_days(terms_code)
            frequency = request.form.get("frequency", "weekly")
            day_of_week = parse_int(request.form.get("day_of_week"), 0, 0, 6)
            day_of_month = parse_int(request.form.get("day_of_month"), 1, 1, 28)
            active = 1 if request.form.get("active") == "on" else 0

            if not customer_name:
                conn.close()
                flash("Customer name is required.", "error")
                return redirect(url_for("customers"))

            cur.execute(
                "SELECT id FROM recipients WHERE lower(group_name) = lower(?)",
                (customer_name,),
            )
            if cur.fetchone():
                conn.close()
                flash("A customer or group with that name already exists.", "error")
                return redirect(url_for("customers"))

            cur.execute(
                "INSERT INTO recipients(group_name, recipient_type, email_to, net_terms, terms_code, frequency, day_of_week, day_of_month, active, created_at) "
                "VALUES (?, 'single', ?, ?, ?, ?, ?, ?, ?, ?)",
                (customer_name, email_to, net_terms, terms_code, frequency, day_of_week, day_of_month, active, now),
            )
            conn.commit()
            conn.close()
            flash(f"Customer {customer_name} added.", "success")
            return redirect(url_for("customers"))

        if form_type == "create_group":
            group_name = request.form.get("group_name", "").strip()
            email_to = normalize_email_value(request.form.get("email_to", ""))
            terms_code = get_terms_code(request.form.get("terms_code", "net_30"))
            net_terms = get_terms_days(terms_code)
            frequency = request.form.get("frequency", "weekly")
            day_of_week = parse_int(request.form.get("day_of_week"), 0, 0, 6)
            day_of_month = parse_int(request.form.get("day_of_month"), 1, 1, 28)
            active = 1 if request.form.get("active") == "on" else 0

            if not group_name:
                conn.close()
                flash("Group name is required.", "error")
                return redirect(url_for("customers"))

            cur.execute(
                "SELECT id FROM recipients WHERE lower(group_name) = lower(?)",
                (group_name,),
            )
            if cur.fetchone():
                conn.close()
                flash("A customer or group with that name already exists.", "error")
                return redirect(url_for("customers"))

            cur.execute(
                "INSERT INTO recipients(group_name, recipient_type, email_to, net_terms, terms_code, frequency, day_of_week, day_of_month, active, created_at) "
                "VALUES (?, 'group', ?, ?, ?, ?, ?, ?, ?, ?)",
                (group_name, email_to, net_terms, terms_code, frequency, day_of_week, day_of_month, active, now),
            )
            group_id = cur.lastrowid

            member_ids = [parse_int(val, None) for val in request.form.getlist("member_ids")]
            member_ids = [mid for mid in member_ids if mid]
            valid_ids = []
            if member_ids:
                placeholders = ",".join(["?"] * len(member_ids))
                cur.execute(
                    f"SELECT id FROM recipients WHERE recipient_type = 'single' AND id IN ({placeholders})",
                    member_ids,
                )
                valid_ids = [row["id"] for row in cur.fetchall()]

            if valid_ids:
                placeholders = ",".join(["?"] * len(valid_ids))
                cur.execute(f"DELETE FROM group_members WHERE customer_id IN ({placeholders})", valid_ids)
                cur.executemany(
                    "INSERT OR IGNORE INTO group_members(group_id, customer_id, created_at) VALUES (?, ?, ?)",
                    [(group_id, cid, now) for cid in valid_ids],
                )

            conn.commit()
            conn.close()
            flash(f"Group {group_name} created.", "success")
            return redirect(url_for("customers"))

        if form_type == "update_existing":
            new_name = request.form.get("new_name", "").strip()
            existing_id = parse_int(request.form.get("existing_id"), None)
            if not new_name or not existing_id:
                conn.close()
                flash("Select an existing customer to update.", "error")
                return redirect(url_for("customers"))

            cur.execute("SELECT * FROM recipients WHERE id = ?", (existing_id,))
            existing = cur.fetchone()
            if not existing or existing["recipient_type"] != "single":
                conn.close()
                flash("Existing customer not found.", "error")
                return redirect(url_for("customers"))

            cur.execute(
                "SELECT id FROM recipients WHERE lower(group_name) = lower(?) AND id != ?",
                (new_name, existing_id),
            )
            if cur.fetchone():
                conn.close()
                flash("Another customer already uses that name.", "error")
                return redirect(url_for("customers"))

            cur.execute(
                "UPDATE recipients SET group_name = ? WHERE id = ?",
                (new_name, existing_id),
            )
            conn.commit()
            conn.close()
            flash("Customer name updated.", "success")
            return redirect(url_for("customers"))

        if form_type == "merge_single":
            source_id = parse_int(request.form.get("source_id"), None)
            target_id = parse_int(request.form.get("target_id"), None)
            if not source_id or not target_id:
                conn.close()
                flash("Select source and target customers for merge.", "error")
                return redirect(url_for("customers"))
            if source_id == target_id:
                conn.close()
                flash("Source and target must be different customers.", "error")
                return redirect(url_for("customers"))

            cur.execute("SELECT * FROM recipients WHERE id = ?", (source_id,))
            source = cur.fetchone()
            cur.execute("SELECT * FROM recipients WHERE id = ?", (target_id,))
            target = cur.fetchone()
            if not source or not target:
                conn.close()
                flash("Source or target customer not found.", "error")
                return redirect(url_for("customers"))
            if source["recipient_type"] != "single" or target["recipient_type"] != "single":
                conn.close()
                flash("Merge is only available for single customers.", "error")
                return redirect(url_for("customers"))

            now_merge = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            source_name = normalize_name(source["group_name"])
            target_name = normalize_name(target["group_name"])

            merged_email = normalize_email_value(
                ",".join([target["email_to"] or "", source["email_to"] or ""])
            )
            if merged_email != normalize_email_value(target["email_to"]):
                cur.execute(
                    "UPDATE recipients SET email_to = ? WHERE id = ?",
                    (merged_email, target_id),
                )

            cur.execute(
                "UPDATE customer_aliases SET recipient_id = ? WHERE recipient_id = ?",
                (target_id, source_id),
            )
            if name_key(source_name) and name_key(source_name) != name_key(target_name):
                cur.execute(
                    "INSERT INTO customer_aliases(alias_name, recipient_id, created_at) VALUES (?, ?, ?) "
                    "ON CONFLICT(alias_name) DO UPDATE SET recipient_id = excluded.recipient_id",
                    (source_name, target_id, now_merge),
                )

            cur.execute("SELECT group_id FROM group_members WHERE customer_id = ?", (source_id,))
            source_group_rows = cur.fetchall()
            if source_group_rows:
                cur.executemany(
                    "INSERT OR IGNORE INTO group_members(group_id, customer_id, created_at) VALUES (?, ?, ?)",
                    [(row["group_id"], target_id, now_merge) for row in source_group_rows],
                )
            cur.execute("DELETE FROM group_members WHERE customer_id = ?", (source_id,))
            cur.execute("UPDATE statement_runs SET recipient_id = ? WHERE recipient_id = ?", (target_id, source_id))
            cur.execute("UPDATE notice_sends SET recipient_id = ? WHERE recipient_id = ?", (target_id, source_id))
            cur.execute("UPDATE customer_mappings SET recipient_id = ? WHERE recipient_id = ?", (target_id, source_id))
            cur.execute("DELETE FROM recipients WHERE id = ?", (source_id,))

            conn.commit()
            conn.close()
            flash(
                f"Merged {source_name} into {target_name}. Alias saved for statement matching.",
                "success",
            )
            return redirect(url_for("customers"))

        if form_type == "bulk_update":
            upload = request.files.get("bulk_file")
            conn.close()
            try:
                added, updated, skipped, skipped_details = import_bulk_customers_from_upload(upload)
                flash(f"Bulk update complete. Added {added}, updated {updated}, skipped {skipped}.", "success")
                if skipped_details:
                    preview = "; ".join(skipped_details[:5])
                    if len(skipped_details) > 5:
                        preview += "; ..."
                    flash(f"Skipped details: {preview}", "error")
            except Exception as exc:
                flash(f"Bulk update failed: {exc}", "error")
            return redirect(url_for("customers"))

        conn.close()
        flash("Unknown action.", "error")
        return redirect(url_for("customers"))

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM recipients ORDER BY group_name ASC")
    rows = cur.fetchall()
    conn.close()

    singles_all = [r for r in rows if r["recipient_type"] == "single"]
    groups_all = [r for r in rows if r["recipient_type"] == "group"]
    active_singles = [r for r in singles_all if r["active"]]
    inactive_singles = [r for r in singles_all if not r["active"]]
    active_groups = [r for r in groups_all if r["active"]]
    inactive_groups = [r for r in groups_all if not r["active"]]
    grouped_single_ids = get_grouped_customer_ids()
    active_single_only = [r for r in active_singles if r["id"] not in grouped_single_ids]

    members_by_group = get_group_members_by_group_id()
    group_name_map = {g["id"]: g["group_name"] for g in groups_all}
    group_member_counts = {gid: len(members) for gid, members in members_by_group.items()}
    group_member_names = {
        gid: ", ".join([m["name"] for m in members]) for gid, members in members_by_group.items()
    }

    customer_groups = {}
    for gid, members in members_by_group.items():
        group_name = group_name_map.get(gid)
        if not group_name:
            continue
        for member in members:
            customer_groups[member["id"]] = group_name

    new_customers = []
    invoice_label = None
    try:
        unique_names, invoice_label = get_latest_invoice_customer_names()
        existing_keys = get_all_single_name_keys()
        new_customers = [name for name in unique_names if name_key(name) not in existing_keys]
    except Exception as exc:
        flash(f"Unable to load latest invoice file for new customers: {exc}", "error")

    term_labels = {code: label for code, label in TERM_OPTIONS}
    return render_template(
        "customers.html",
        new_customers=new_customers,
        groups=active_groups,
        singles=active_singles,
        single_only=active_single_only,
        all_recipients=active_groups + active_singles,
        inactive_recipients=inactive_groups + inactive_singles,
        all_singles=singles_all,
        group_member_counts=group_member_counts,
        group_member_names=group_member_names,
        customer_groups=customer_groups,
        term_options=TERM_OPTIONS,
        term_labels=term_labels,
        invoice_label=invoice_label,
    )


@app.route("/customers/<int:recipient_id>/emails", methods=["POST"])
@app.route("/recipients/<int:recipient_id>/emails", methods=["POST"])
def update_customer_emails(recipient_id):
    email_to = normalize_email_value(request.form.get("email_to", ""))
    if not email_to:
        return jsonify({"ok": False, "error": "Email is required"}), 400

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM recipients WHERE id = ?", (recipient_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return jsonify({"ok": False, "error": "Customer not found"}), 404

    cur.execute("UPDATE recipients SET email_to = ? WHERE id = ?", (email_to, recipient_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "email_to": email_to})


@app.route("/customers/<int:recipient_id>/edit", methods=["GET", "POST"])
@app.route("/recipients/<int:recipient_id>/edit", methods=["GET", "POST"])
def edit_customer(recipient_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
    recipient = cur.fetchone()
    if not recipient:
        conn.close()
        flash("Customer not found.", "error")
        return redirect(url_for("customers"))

    if request.method == "POST":
        group_name = request.form.get("group_name", "").strip()
        email_to = normalize_email_value(request.form.get("email_to", ""))
        terms_code = get_terms_code(request.form.get("terms_code", "net_30"))
        net_terms = get_terms_days(terms_code)
        frequency = request.form.get("frequency", "weekly")
        day_of_week = parse_int(request.form.get("day_of_week"), 0, 0, 6)
        day_of_month = parse_int(request.form.get("day_of_month"), 1, 1, 28)
        active = 1 if request.form.get("active") == "on" else 0

        if not group_name:
            conn.close()
            flash("Name is required.", "error")
            return redirect(url_for("edit_customer", recipient_id=recipient_id))

        cur.execute(
            "SELECT id FROM recipients WHERE lower(group_name) = lower(?) AND id != ?",
            (group_name, recipient_id),
        )
        if cur.fetchone():
            conn.close()
            flash("Another customer or group already uses that name.", "error")
            return redirect(url_for("edit_customer", recipient_id=recipient_id))

        cur.execute(
            "UPDATE recipients SET group_name = ?, email_to = ?, net_terms = ?, terms_code = ?, "
            "frequency = ?, day_of_week = ?, day_of_month = ?, active = ? WHERE id = ?",
            (group_name, email_to, net_terms, terms_code, frequency, day_of_week, day_of_month, active, recipient_id),
        )

        if recipient["recipient_type"] == "group":
            member_ids = [parse_int(val, None) for val in request.form.getlist("member_ids")]
            member_ids = [mid for mid in member_ids if mid]
            valid_ids = []
            if member_ids:
                placeholders = ",".join(["?"] * len(member_ids))
                cur.execute(
                    f"SELECT id FROM recipients WHERE recipient_type = 'single' AND id IN ({placeholders})",
                    member_ids,
                )
                valid_ids = [row["id"] for row in cur.fetchall()]

            cur.execute("DELETE FROM group_members WHERE group_id = ?", (recipient_id,))
            if valid_ids:
                placeholders = ",".join(["?"] * len(valid_ids))
                cur.execute(f"DELETE FROM group_members WHERE customer_id IN ({placeholders})", valid_ids)
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cur.executemany(
                    "INSERT OR IGNORE INTO group_members(group_id, customer_id, created_at) VALUES (?, ?, ?)",
                    [(recipient_id, cid, now) for cid in valid_ids],
                )

        conn.commit()
        conn.close()
        flash("Customer updated.", "success")
        return redirect(url_for("customers"))

    if recipient["recipient_type"] == "group":
        cur.execute(
            "SELECT id, group_name FROM recipients WHERE recipient_type = 'single' AND active = 1 ORDER BY group_name ASC"
        )
        singles = cur.fetchall()
        conn.close()
        members_by_group = get_group_members_by_group_id()
        member_ids = {m["id"] for m in members_by_group.get(recipient_id, [])}
        term_labels = {code: label for code, label in TERM_OPTIONS}
        return render_template(
            "group_edit.html",
            recipient=recipient,
            singles=singles,
            member_ids=member_ids,
            term_options=TERM_OPTIONS,
            term_labels=term_labels,
        )

    conn.close()
    term_labels = {code: label for code, label in TERM_OPTIONS}
    return render_template(
        "customer_edit.html",
        recipient=recipient,
        term_options=TERM_OPTIONS,
        term_labels=term_labels,
    )


@app.route("/customers/<int:recipient_id>/delete", methods=["POST"])
@app.route("/recipients/<int:recipient_id>/delete", methods=["POST"])
def delete_customer(recipient_id):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM group_members WHERE group_id = ? OR customer_id = ?", (recipient_id, recipient_id))
    cur.execute("DELETE FROM customer_aliases WHERE recipient_id = ?", (recipient_id,))
    cur.execute("DELETE FROM recipients WHERE id = ?", (recipient_id,))
    conn.commit()
    conn.close()
    flash("Customer deleted.", "success")
    return redirect(url_for("customers"))


@app.route("/mappings")
def mappings():
    flash("Mappings are now managed in Customers > Groups.", "success")
    return redirect(url_for("customers"))


@app.route("/uploads", methods=["GET", "POST"])
def uploads():
    if request.method == "POST":
        file = request.files.get("invoice_file")
        if not file or not file.filename:
            flash("Please choose a file", "error")
            return redirect(url_for("uploads"))

        os.makedirs(UPLOAD_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{file.filename}"
        path = os.path.join(UPLOAD_DIR, filename)
        file.save(path)

        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO invoice_files(filename, path, uploaded_at) VALUES (?, ?, ?)",
            (file.filename, path, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )
        conn.commit()
        conn.close()
        flash("Invoice file uploaded", "success")
        return redirect(url_for("uploads"))

    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM invoice_files ORDER BY uploaded_at DESC LIMIT 20")
    files = cur.fetchall()
    conn.close()
    return render_template("uploads.html", files=files)


@app.route("/send", methods=["GET", "POST"])
def send_manual():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT * FROM recipients WHERE recipient_type = 'group' ORDER BY group_name ASC")
    groups = cur.fetchall()
    cur.execute("SELECT * FROM recipients WHERE recipient_type = 'single' ORDER BY group_name ASC")
    singles = cur.fetchall()
    cur.execute("SELECT * FROM invoice_files ORDER BY uploaded_at DESC LIMIT 20")
    invoice_files = cur.fetchall()
    conn.close()

    if request.method == "POST":
        recipient_id = request.form.get("recipient_id")
        if not recipient_id:
            flash("Please select a customer or group.", "error")
            return redirect(url_for("send_manual"))
        recipient_id = int(recipient_id)
        invoice_file_id = request.form.get("invoice_file_id")
        if invoice_file_id:
            invoice_file_id = int(invoice_file_id)
        action = request.form.get("action", "send")

        try:
            if invoice_file_id:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT path FROM invoice_files WHERE id = ?", (invoice_file_id,))
                row = cur.fetchone()
                conn.close()
                if not row:
                    raise RuntimeError("Invoice file not found")
                invoice_path = row[0]
            else:
                invoice_file_id, invoice_path = get_invoice_for_run()

            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
            recipient = cur.fetchone()
            conn.close()
            if not recipient:
                raise RuntimeError("Recipient not found")
            recipient = dict(recipient)
            recipient = ensure_recipient_email(recipient, request.form.get("email_to", ""))

            run_for_recipient(recipient, invoice_path, invoice_file_id, "manual")
            flash("Statement sent", "success")
        except Exception as e:
            flash(f"Send failed: {e}", "error")

        return redirect(url_for("send_manual"))

    return render_template("send.html", groups=groups, singles=singles, invoice_files=invoice_files)


@app.route("/send/download")
def send_download():
    recipient_id = request.args.get("recipient_id")
    invoice_file_id = request.args.get("invoice_file_id")
    if not recipient_id:
        return "Recipient is required", 400
    recipient_id = int(recipient_id)
    if invoice_file_id:
        invoice_file_id = int(invoice_file_id)

    try:
        if invoice_file_id:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("SELECT path FROM invoice_files WHERE id = ?", (invoice_file_id,))
            row = cur.fetchone()
            conn.close()
            if not row:
                return "Invoice file not found", 404
            invoice_path = row[0]
        else:
            _, invoice_path = get_invoice_for_run()

        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT * FROM recipients WHERE id = ?", (recipient_id,))
        recipient = cur.fetchone()
        conn.close()
        if not recipient:
            return "Recipient not found", 404

        output_path = build_statement_pdf(recipient, invoice_path)
        return send_file(
            output_path,
            as_attachment=False,
            download_name=os.path.basename(output_path),
            mimetype="application/pdf",
        )
    except Exception as exc:
        return f"Statement download failed: {exc}", 400


@app.route("/settings", methods=["GET", "POST"])
def settings():
    template_keys = ["statement", "overdue", "skipped", "short_paid"]
    allowed_tabs = {"general", "templates"}
    if request.method == "POST":
        form_type = request.form.get("form_type", "general")
        if form_type == "email_templates":
            for template_key in template_keys:
                setting_key = f"email_template_{template_key}"
                value = request.form.get(setting_key, EMAIL_TEMPLATE_DEFAULTS.get(template_key, ""))
                value = value.replace("\r\n", "\n")
                set_setting(setting_key, value)
            flash("Email templates saved.", "success")
            return redirect(url_for("settings", tab="templates"))

        fields = [
            "smtp_host",
            "smtp_port",
            "smtp_user",
            "smtp_pass",
            "smtp_from",
            "smtp_tls",
            "smtp_timeout",
            "scheduled_send_delay_seconds",
            "scheduled_send_retries",
            "scheduled_retry_backoff_seconds",
            "scheduled_max_recipients",
            "company_name",
            "company_subtitle",
            "company_address",
            "company_phone",
            "company_email",
            "company_website",
            "cc_statement",
            "cc_overdue",
            "cc_skipped",
            "cc_short_paid",
            "invoice_source",
            "invoice_path",
        ]
        for key in fields:
            value = request.form.get(key, "")
            if key == "smtp_tls":
                value = "true" if value == "on" else "false"
            set_setting(key, value)

        logo_file = request.files.get("logo_file")
        if logo_file and logo_file.filename:
            try:
                logo_path = save_logo_file(logo_file)
                set_setting("logo_path", logo_path)
            except Exception as exc:
                flash(f"Logo upload failed: {exc}", "error")
        flash("Settings saved", "success")
        return redirect(url_for("settings", tab="general"))

    settings_data = {key: get_setting(key, "") for key in [
        "smtp_host",
        "smtp_port",
        "smtp_user",
        "smtp_pass",
        "smtp_from",
        "smtp_tls",
        "smtp_timeout",
        "scheduled_send_delay_seconds",
        "scheduled_send_retries",
        "scheduled_retry_backoff_seconds",
        "scheduled_max_recipients",
        "company_name",
        "company_subtitle",
        "company_address",
        "company_phone",
        "company_email",
        "company_website",
        "cc_statement",
        "cc_overdue",
        "cc_skipped",
        "cc_short_paid",
        "logo_path",
        "invoice_source",
        "invoice_path",
    ]}
    email_templates = {key: get_email_template_body(key) for key in template_keys}
    active_tab = request.args.get("tab", "general")
    if active_tab not in allowed_tabs:
        active_tab = "general"
    return render_template(
        "settings.html",
        settings=settings_data,
        email_templates=email_templates,
        active_tab=active_tab,
    )


@app.route("/run-scheduled", methods=["POST"])
def run_scheduled():
    try:
        ensure_schedule_worker_running()
        job_id, error = create_scheduled_job(session.get("username", "system"))
        if error:
            flash(error, "error")
        else:
            flash(
                f"Scheduled run queued (Job #{job_id}). Processing in background, one customer at a time.",
                "success",
            )
    except Exception as e:
        flash(f"Could not queue scheduled run: {e}", "error")
    return redirect(url_for("index"))


if __name__ == "__main__":
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(LOGO_DIR, exist_ok=True)
    os.makedirs(OUT_DIR, exist_ok=True)
    init_db()
    app.run(debug=True)
