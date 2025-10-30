# ==========================================
# Expense Manager (Streamlit + SQLite)
# Bản đã hợp nhất & chỉnh sửa theo yêu cầu mới
# ==========================================

import streamlit as st
import sqlite3, hashlib, pandas as pd, datetime as dt, altair as alt
from pathlib import Path
import random, re, unicodedata, io, math  # <-- thêm math
from typing import Tuple

DB_PATH = "expense.db"
ENABLE_DEMO = True

# ---------- Helpers tiền tệ / thời gian ----------
def format_vnd(n):
    try:
        return f"{float(n):,.0f}".replace(",", ".")
    except Exception:
        return str(n)

def parse_vnd_str(s):
    """
    Cho phép nhập có dấu chấm/phẩy/khoảng trắng.
    Ví dụ: '5.000.000' -> 5000000.0
    """
    if s is None:
        return 0.0
    digits = re.sub(r"[^\d]", "", str(s))
    try:
        return float(digits) if digits else 0.0
    except Exception:
        return 0.0

def join_date_time(d: dt.date, t: dt.time) -> str:
    return dt.datetime.combine(d, t.replace(second=0, microsecond=0)).strftime("%Y-%m-%d %H:%M")

def strip_accents_lower(s):
    if s is None:
        return ""
    s = unicodedata.normalize("NFD", str(s))
    return "".join(ch for ch in s if unicodedata.category(ch) != "Mn").lower()

# ==== Notices (thông báo đứng lại đủ lâu) ====
def show_notice(msg: str, level: str = "success"):
    """Ghim notice (success/info/error) cho lần render hiện tại."""
    st.session_state["__inline_notice__"] = (msg, level)

def render_inline_notice():
    """Hiển thị notice nếu có và tự clear ở lần rerun kế tiếp."""
    note = st.session_state.pop("__inline_notice__", None)
    if note:
        msg, level = note
        if level == "error":
            st.error(msg)
        elif level == "info":
            st.info(msg)
        else:
            st.success(msg)

def _toast_ok(msg: str):
    # Streamlit toast không chỉnh thời lượng -> kết hợp toast + inline notice
    try:
        st.toast(msg)
    except Exception:
        pass
    show_notice(msg, "success")

# Ô nhập tiền có auto chèn dấu chấm
def money_input(label: str, key: str, placeholder: str = "VD: 5.000.000"):
    raw = st.text_input(label, key=key, placeholder=placeholder)
    cleaned = re.sub(r"[^\d]", "", raw or "")
    if cleaned and raw and raw != "." and cleaned != raw.replace(".", ""):
        pretty = f"{int(cleaned):,}".replace(",", ".")
        st.session_state[key] = pretty
    return parse_vnd_str(st.session_state.get(key, raw))

# Khoảng hiển thị cho Tháng/Năm/Tuần
def start_months_back(end_date: dt.date, months: int) -> dt.date:
    idx = end_date.year * 12 + (end_date.month - 1) - (months - 1)
    y0 = idx // 12
    m0 = idx % 12 + 1
    return dt.date(y0, m0, 1)

def year_window(end_date: dt.date, years: int):
    y2 = end_date.year
    y1 = y2 - (years - 1)
    return dt.date(y1, 1, 1), dt.date(y2, 12, 31)

def start_weeks_back(end_date: dt.date, weeks: int) -> dt.date:
    return end_date - dt.timedelta(days=7*(weeks-1))

# ---------- DB ----------
def get_conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c

def hash_password(pw): return hashlib.sha256(pw.encode("utf-8")).hexdigest()
def get_df(q, p=()): c=get_conn(); df=pd.read_sql_query(q, c, params=p); c.close(); return df
def execute(q, p=()): c=get_conn(); c.execute(q, p); c.commit(); c.close()
def fetchone(q, p=()): c=get_conn(); r=c.execute(q, p).fetchone(); c.close(); return r
def exec_script(c, s): c.executescript(s); c.commit()

INIT_SQL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS users(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 email TEXT UNIQUE NOT NULL,
 password_hash TEXT NOT NULL,
 created_at TEXT NOT NULL,
 display_name TEXT,
 onboarded INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS accounts(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 user_id INTEGER NOT NULL,
 name TEXT NOT NULL,
 type TEXT NOT NULL,
 currency TEXT NOT NULL DEFAULT 'VND',
 opening_balance REAL NOT NULL DEFAULT 0,
 created_at TEXT NOT NULL,
 FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS categories(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 user_id INTEGER NOT NULL,
 name TEXT NOT NULL,
 type TEXT NOT NULL,
 parent_id INTEGER,
 FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS transactions(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 user_id INTEGER NOT NULL,
 account_id INTEGER NOT NULL,
 type TEXT NOT NULL,
 category_id INTEGER,
 amount REAL NOT NULL,
 currency TEXT NOT NULL DEFAULT 'VND',
 fx_rate REAL,
 merchant_id INTEGER,
 notes TEXT,
 tags TEXT,
 occurred_at TEXT NOT NULL,
 created_at TEXT NOT NULL,
 FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
 FOREIGN KEY(account_id) REFERENCES accounts(id) ON DELETE CASCADE,
 FOREIGN KEY(category_id) REFERENCES categories(id) ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS budgets(
 id INTEGER PRIMARY KEY AUTOINCREMENT,
 user_id INTEGER NOT NULL,
 category_id INTEGER NOT NULL,
 amount REAL NOT NULL,
 start_date TEXT NOT NULL,
 end_date TEXT NOT NULL,
 FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
);
"""

def init_db():
    Path(DB_PATH).touch(exist_ok=True)
    c = get_conn()
    exec_script(c, INIT_SQL)
    if ENABLE_DEMO:
        seed_demo_user_once(c)
    c.close()

# ---------- Auth ----------
def create_user(email, pw):
    c = get_conn()
    try:
        c.execute("INSERT INTO users(email,password_hash,created_at,onboarded) VALUES(?,?,?,0)",
                  (email.lower(), hash_password(pw), dt.datetime.now().isoformat()))
        c.commit()
        uid = c.execute("SELECT id FROM users WHERE email=?", (email.lower(),)).fetchone()["id"]
        now = dt.datetime.now().isoformat()
        c.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                  (uid, "Tiền mặt", "cash", "VND", 0, now))
        c.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                  (uid, "Tài khoản ngân hàng", "bank", "VND", 0, now))
        c.commit()
        ok, msg = True, "Tạo tài khoản thành công!"
    except sqlite3.IntegrityError:
        ok, msg = False, "Email đã tồn tại."
    finally:
        c.close()
    return ok, msg

def login_user(email, pw):
    r = fetchone("SELECT id,password_hash FROM users WHERE email=?", (email.lower(),))
    return (r["id"] if r and r["password_hash"] == hash_password(pw) else None)

def get_user(uid): return fetchone("SELECT * FROM users WHERE id=?", (uid,))
def set_user_profile(uid, name): execute("UPDATE users SET display_name=? WHERE id=?", (name.strip(), uid))
def finish_onboarding(uid): execute("UPDATE users SET onboarded=1 WHERE id=?", (uid,))

# ---------- Seed DEMO ----------
def seed_demo_user_once(c):
    if not c.execute("SELECT 1 FROM users WHERE email='demo@expense.local'").fetchone():
        now = dt.datetime.now().isoformat()
        c.execute(
            "INSERT INTO users(email,password_hash,created_at,display_name,onboarded) VALUES(?,?,?,?,1)",
            ("demo@expense.local", hash_password("demo1234"), now, "Tài khoản DEMO")
        )
        c.commit()

    uid = c.execute("SELECT id FROM users WHERE email='demo@expense.local'").fetchone()["id"]
    now = dt.datetime.now().isoformat()

    if not c.execute("SELECT 1 FROM accounts WHERE user_id=?", (uid,)).fetchone():
        c.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                  (uid, "Tiền mặt", "cash", "VND", 2_000_000, now))
        c.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                  (uid, "Tài khoản ngân hàng", "bank", "VND", 8_000_000, now))

    base_cats = [
        ("Ăn uống","expense"), ("Cà phê","expense"), ("Giải trí","expense"),
        ("Tiền học","expense"), ("Đi lại","expense"), ("Mua sắm","expense"),
        ("Lương","income"), ("Thưởng","income"), ("Bán đồ cũ","income")
    ]
    for n,t in base_cats:
        if not c.execute("SELECT 1 FROM categories WHERE user_id=? AND name=? AND type=?",(uid,n,t)).fetchone():
            c.execute("INSERT INTO categories(user_id,name,type) VALUES(?,?,?)",(uid,n,t))

    acc_ids = [r["id"] for r in c.execute("SELECT id FROM accounts WHERE user_id=?", (uid,)).fetchall()]
    exp_ids = [r["id"] for r in c.execute("SELECT id FROM categories WHERE user_id=? AND type='expense'", (uid,)).fetchall()]
    inc_ids = [r["id"] for r in c.execute("SELECT id FROM categories WHERE user_id=? AND type='income'", (uid,)).fetchall()]

    def add_month_data(y, m):
        month_mid = dt.date(y, m, 15)
        for _ in range(random.randint(3, 5)):  # incomes
            cat = random.choice(inc_ids)
            amt = random.choice([random.randint(6_000_000, 18_000_000),
                                 random.randint(500_000, 2_000_000)])
            day_off = random.randint(-10, 10)
            hh, mm = random.randint(8, 21), random.randint(0, 59)
            occurred = dt.datetime.combine(month_mid + dt.timedelta(days=day_off),
                                           dt.time(hh, mm)).strftime("%Y-%m-%d %H:%M")
            c.execute("""INSERT INTO transactions(user_id,account_id,type,category_id,amount,currency,occurred_at,created_at)
                         VALUES(?,?,?,?,?,?,?,?)""",
                      (uid, random.choice(acc_ids), "income", cat, amt, "VND", occurred, now))
        for _ in range(random.randint(14, 22)):  # expenses
            cat = random.choice(exp_ids)
            amt = random.choice([random.randint(80_000, 350_000),
                                 random.randint(300_000, 1_200_000),
                                 random.randint(1_500_000, 6_000_000)])
            day_off = random.randint(-13, 13)
            hh, mm = random.randint(8, 22), random.randint(0, 59)
            occurred = dt.datetime.combine(month_mid + dt.timedelta(days=day_off),
                                           dt.time(hh, mm)).strftime("%Y-%m-%d %H:%M")
            c.execute("""INSERT INTO transactions(user_id,account_id,type,category_id,amount,currency,occurred_at,created_at)
                         VALUES(?,?,?,?,?,?,?,?)""",
                      (uid, random.choice(acc_ids), "expense", cat, amt, "VND", occurred, now))

    c.execute("DELETE FROM transactions WHERE user_id=?", (uid,))
    for y in (2023, 2024):
        for m in range(1, 12+1):
            add_month_data(y, m)
    today = dt.date.today()
    for m in range(1, today.month + 1):
        add_month_data(today.year, m)
    c.commit()

    cats_map = {r["name"]: r["id"] for r in c.execute(
        "SELECT id,name FROM categories WHERE user_id=? AND type='expense'", (uid,)
    ).fetchall()}
    budget_templates = {"Ăn uống": 4_500_000, "Cà phê": 1_200_000, "Giải trí": 2_500_000, "Tiền học": 6_000_000}

    c.execute("DELETE FROM budgets WHERE user_id=?", (uid,))
    anchor = dt.date.today().replace(day=1)
    for i in range(12):
        first = (anchor - dt.timedelta(days=30*i)).replace(day=1)
        next_month = (first.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        last = next_month - dt.timedelta(days=1)
        for name, amt in budget_templates.items():
            cid = cats_map.get(name)
            if not cid: continue
            c.execute("""INSERT INTO budgets(user_id,category_id,amount,start_date,end_date)
                         VALUES(?,?,?,?,?)""",
                      (uid, int(cid), float(amt), str(first), str(last)))
    c.commit()

# ---------- Data utils ----------
TYPE_LABELS_VN = {"expense":"Chi tiêu", "income":"Thu nhập"}
COLOR_INCOME = "#2ecc71"
COLOR_EXPENSE = "#ff6b6b"
COLOR_NET = "#06b6d4"

def list_transactions(uid, d1=None, d2=None):
    q = """SELECT t.id, t.occurred_at, t.type, t.amount, t.currency,
                  a.name AS account, c.name AS category, t.notes, t.tags, t.merchant_id AS merchant
           FROM transactions t JOIN accounts a ON a.id=t.account_id
           LEFT JOIN categories c ON c.id=t.category_id
           WHERE t.user_id=?"""
    p=[uid]
    if d1: q+=" AND date(t.occurred_at)>=date(?)"; p.append(str(d1))
    if d2: q+=" AND date(t.occurred_at)<=date(?)"; p.append(str(d2))
    q += " ORDER BY t.occurred_at DESC, t.id DESC"
    return get_df(q, tuple(p))

def df_tx_vi(df):
    if df is None or df.empty: return df
    m={"id":"ID","occurred_at":"Thời điểm","type":"Loại","amount":"Số tiền","currency":"Tiền tệ",
       "account":"Ví / Tài khoản","category":"Danh mục","notes":"Ghi chú","tags":"Thẻ","merchant":"Nơi chi tiêu"}
    df=df.rename(columns={k:v for k,v in m.items() if k in df.columns}).copy()
    if "Loại" in df.columns:
        df["Loại"]=df["Loại"].map({"expense":"Chi tiêu","income":"Thu nhập"}).fillna(df["Loại"])
    if "Số tiền" in df.columns:
        df["Số tiền"]=df["Số tiền"].map(format_vnd)
    return df

def get_accounts(uid): return get_df("SELECT * FROM accounts WHERE user_id=?", (uid,))
def get_categories(uid, t=None):
    q="SELECT * FROM categories WHERE user_id=?"; p=[uid]
    if t: q+=" AND type=?"; p.append(t)
    q+=" ORDER BY name"; return get_df(q, tuple(p))

def add_transaction(uid, account_id, ttype, cat_id, amount, notes, occurred_dt):
    execute("""INSERT INTO transactions(user_id,account_id,type,category_id,amount,currency,occurred_at,created_at)
               VALUES(?,?,?,?,?,?,?,?)""",
            (uid,account_id,ttype,cat_id,amount,"VND",occurred_dt,dt.datetime.now().isoformat()))

def add_category(uid,name,t,parent_id=None):
    execute("INSERT INTO categories(user_id,name,type,parent_id) VALUES(?,?,?,?)",(uid,name.strip(),t,parent_id))

def add_account(uid,name,t,balance):
    execute("INSERT INTO accounts(user_id,name,type,opening_balance,created_at) VALUES(?,?,?,?,?)",
            (uid,name.strip(),t,balance,dt.datetime.now().isoformat()))

def delete_transaction(uid, tx_id: int):
    execute("DELETE FROM transactions WHERE user_id=? AND id=?", (uid, int(tx_id)))

def delete_budget(uid, bid: int):
    execute("DELETE FROM budgets WHERE user_id=? AND id=?", (uid, int(bid)))

def delete_category(uid, cid: int):
    # Xoá budgets liên quan, set NULL category_id cho transactions, set NULL parent của con
    execute("DELETE FROM budgets WHERE user_id=? AND category_id=?", (uid, int(cid)))
    execute("UPDATE transactions SET category_id=NULL WHERE user_id=? AND category_id=?", (uid, int(cid)))
    execute("UPDATE categories SET parent_id=NULL WHERE user_id=? AND parent_id=?", (uid, int(cid)))
    execute("DELETE FROM categories WHERE user_id=? AND id=?", (uid, int(cid)))

# ---------- Table helpers (ẩn ID + sort đúng + STT đánh sau sort) ----------
META_DROP = {"id","user_id","parent_id","ID","user_id","parent_id"}

def _detect_sort_kind(df: pd.DataFrame, col: str) -> str:
    if col == "Loại":
        return "type"
    if col in ("Thời điểm","Ngày giao dịch","Từ ngày","Đến ngày"):
        return "time"
    norm = strip_accents_lower(col)
    if any(k in norm for k in ["tien","dư","du","muc","hạn","han","so"]):
        return "number"
    return "text"

def _type_key_series(s: pd.Series) -> pd.Series:
    def to_key(x: str) -> int:
        x = str(x)
        x = x.replace("🟢","").replace("🔴","").strip()
        x_no = strip_accents_lower(x)
        if "chi tieu" in x_no:
            return 0
        if "thu nhap" in x_no:
            return 1
        return 2
    return s.astype(str).map(to_key)

def sort_df_for_display(df: pd.DataFrame, sort_col: str, ascending: bool):
    if df is None or df.empty or sort_col not in df.columns:
        return df
    kind = _detect_sort_kind(df, sort_col)
    if kind == "type":
        key_func = _type_key_series
        ascending = True
    elif kind == "time":
        key_func = lambda s: pd.to_datetime(s, errors="coerce")
    elif kind == "number":
        key_func = lambda s: pd.to_numeric(
            s.astype(str).str.replace(".","",regex=False).str.replace(",","",regex=False).str.strip(),
            errors="coerce"
        ).fillna(0.0)
    else:
        key_func = lambda s: s.astype(str).map(strip_accents_lower)
    return df.sort_values(by=sort_col, ascending=ascending, key=key_func, kind="mergesort")

def render_table(
    df: pd.DataFrame,
    default_sort_col: str | None = None,
    default_asc: bool = False,
    height: int = 320,
    key_suffix: str = "",
    exclude_sort_cols: set[str] | None = None,
    show_type_filters: bool = True,
    show_sort: bool = True,
):
    if df is None or df.empty:
        st.info("Chưa có dữ liệu.")
        return

    df = df.drop(columns=[c for c in df.columns if c in META_DROP], errors="ignore").copy()

    # Lọc theo 'Loại' (nếu cho phép)
    if show_type_filters and ("Loại" in df.columns):
        state_key = f"filter_{key_suffix}"
        if state_key not in st.session_state:
            st.session_state[state_key] = "Tất cả"

        b_all, b_exp, b_inc = st.columns([1, 1, 1])
        if b_all.button("⚪ Tất cả", key=f"all_{key_suffix}"):
            st.session_state[state_key] = "Tất cả"
        if b_exp.button("🔴 Chỉ Chi tiêu", key=f"exp_{key_suffix}"):
            st.session_state[state_key] = "Chi tiêu"
        if b_inc.button("🟢 Chỉ Thu nhập", key=f"inc_{key_suffix}"):
            st.session_state[state_key] = "Thu nhập"

        pick = st.session_state[state_key]
        if pick != "Tất cả":
            df = df[df["Loại"].astype(str).str.contains(pick, case=False, na=False)]

    # Không hiển thị UI sắp xếp
    if not show_sort:
        df_show = df.copy()
        df_show.insert(0, "STT", range(1, len(df_show) + 1))
        st.dataframe(df_show, use_container_width=True, height=height, hide_index=True)
        return

    cols = [c for c in df.columns if (exclude_sort_cols is None or c not in exclude_sort_cols)]
    if not cols:
        df.insert(0, "STT", range(1, len(df) + 1))
        st.dataframe(df, use_container_width=True, height=height, hide_index=True)
        return

    c1, c2, _ = st.columns([1.6, 1.2, 2])
    idx = cols.index(default_sort_col) if default_sort_col in cols else 0
    sort_col = c1.selectbox("Sắp xếp theo", cols, index=idx, key=f"sort_{key_suffix}")
    kind = _detect_sort_kind(df, sort_col)

    if kind == "type":
        st.caption("Thứ tự 'Loại' cố định: Chi tiêu → Thu nhập")
        ascending = True
    else:
        if kind == "time":
            labels = ["Mới nhất", "Cũ nhất"]
        elif kind == "number":
            labels = ["Cao → Thấp", "Thấp → Cao"]
        else:
            labels = ["A → Z", "Z → A"]

        pick = c2.radio("Thứ tự", labels, horizontal=True, key=f"order_{key_suffix}")
        if labels == ["Mới nhất", "Cũ nhất"]:
            ascending = (pick == "Cũ nhất")
        elif labels == ["Cao → Thấp", "Thấp → Cao"]:
            ascending = (pick == "Thấp → Cao")
        else:
            ascending = (pick == "A → Z")

    df_sorted = sort_df_for_display(df, sort_col, ascending)
    df_sorted.insert(0, "STT", range(1, len(df_sorted) + 1))
    st.dataframe(df_sorted, use_container_width=True, height=height, hide_index=True)

# ---------- Aggregations & Delta ----------
def period_sum(uid:int, d1:dt.date, d2:dt.date) -> Tuple[float,float,float]:
    r = fetchone("""
        SELECT
          COALESCE(SUM(CASE WHEN type='income'  THEN amount END),0) AS income,
          COALESCE(SUM(CASE WHEN type='expense' THEN amount END),0) AS expense
        FROM transactions
        WHERE user_id=? AND date(occurred_at) BETWEEN date(?) AND date(?)""",
        (uid, str(d1), str(d2)))
    income, expense = float(r["income"] or 0), float(r["expense"] or 0)
    return income, expense, (income-expense)

def previous_period(d1:dt.date, d2:dt.date, mode:str) -> Tuple[dt.date,dt.date]:
    # khoảng KPI luôn theo đúng "Từ ngày" - "Đến ngày" đang chọn, chỉ giai đoạn trước phụ thuộc mode để so sánh
    if mode=="day":
        span = (d2 - d1).days + 1
        return d1 - dt.timedelta(days=span), d2 - dt.timedelta(days=span)
    if mode=="week":
        return d1 - dt.timedelta(days=7), d2 - dt.timedelta(days=7)
    if mode=="month":
        y = d1.year; m = d1.month
        first_this = dt.date(y, m, 1)
        prev_last = first_this - dt.timedelta(days=1)
        prev_first = dt.date(prev_last.year, prev_last.month, 1)
        return prev_first, prev_last
    # year
    return dt.date(d1.year-1,1,1), dt.date(d1.year-1,12,31)

def query_agg_expense(uid, d1, d2, mode):
    if mode=="day":
        g="date(occurred_at)"; label="Ngày"; xtype="T"
    elif mode=="week":
        g="strftime('%Y-%W', occurred_at)"; label="Tuần"; xtype="O"
    elif mode=="month":
        g="strftime('%Y-%m', occurred_at)"; label="Tháng"; xtype="O"
    else:
        g="strftime('%Y', occurred_at)"; label="Năm"; xtype="O"
    df = get_df(f"""
        SELECT {g} AS label,
               SUM(CASE WHEN type='expense' THEN amount ELSE 0 END) AS Chi_tieu
        FROM transactions
        WHERE user_id=? AND date(occurred_at) BETWEEN date(?) AND date(?)
        GROUP BY {g} ORDER BY {g}
    """, (uid, str(d1), str(d2)))
    if df.empty:
        df = pd.DataFrame(columns=[label,"Chi_tieu"])
    df = df.rename(columns={"label": label})
    return df, label, xtype

# ---------- Category tree helpers ----------
def build_category_tree(uid:int, ctype:str):
    df = get_df("SELECT id,name,parent_id FROM categories WHERE user_id=? AND type=? ORDER BY name",(uid,ctype))
    by_parent = {}
    for _,r in df.iterrows():
        pid = int(r["parent_id"]) if pd.notna(r["parent_id"]) else None
        by_parent.setdefault(pid, []).append({"id": int(r["id"]), "name": r["name"]})
    parents = by_parent.get(None, [])
    for p in parents:
        p["children"] = sorted(by_parent.get(p["id"], []), key=lambda x: strip_accents_lower(x["name"]))
    orphans = []
    return parents, orphans

# ---------- Pages ----------
def page_transactions(uid):
    st.subheader("🧾 Thêm giao dịch mới")
    accounts = get_accounts(uid)
    if accounts.empty:
        st.warning("⚠️ Vui lòng tạo ít nhất 1 tài khoản trước khi thêm giao dịch.")
        return

    # Loại giao dịch
    ttype_vi = st.radio("Loại giao dịch", ["Chi tiêu","Thu nhập"], horizontal=True)
    ttype = "expense" if ttype_vi == "Chi tiêu" else "income"

    # Lấy toàn bộ danh mục theo loại
    cats_all = get_categories(uid, ttype)
    if cats_all.empty:
        st.warning("⚠️ Chưa có danh mục phù hợp. Hãy tạo danh mục ở mục 🏷 trước.")
        return

    # --- Danh mục cha (parent_id IS NULL) ---
    parents = cats_all[cats_all["parent_id"].isna()].copy().sort_values("name")
    parent_name = st.selectbox("Danh mục", parents["name"])
    parent_id = int(parents.loc[parents["name"] == parent_name, "id"].iloc[0])

    # --- Danh mục con của danh mục cha đã chọn ---
    children = cats_all[cats_all["parent_id"] == parent_id].copy().sort_values("name")
    child_label = ["(Không)"] + (children["name"].tolist() if not children.empty else [])
    child_pick = st.selectbox("Danh mục con (nếu có)", child_label, index=0)

    # Quyết định category_id để ghi vào DB
    if child_pick == "(Không)" or children.empty:
        category_id = parent_id
    else:
        category_id = int(children.loc[children["name"] == child_pick, "id"].iloc[0])

    # --- Ví/Tài khoản ---
    acc_name = st.selectbox("Chọn ví/tài khoản", accounts["name"])
    acc_id = int(accounts.loc[accounts["name"] == acc_name, "id"].iloc[0])

    # --- Số tiền & ghi chú ---
    amt = money_input("💰 Số tiền (VND)", key="add_tx_amount", placeholder="VD: 5.000.000")
    notes = st.text_input("📝 Ghi chú (tùy chọn)")

    # --- Thời gian ---
    use_now = st.checkbox("Dùng thời gian hiện tại", value=True)
    if use_now:
        occurred_dt = dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    else:
        date = st.date_input("Ngày giao dịch", value=dt.date.today())
        time = st.time_input("Giờ giao dịch", value=dt.datetime.now().time().replace(second=0, microsecond=0))
        occurred_dt = join_date_time(date, time)

    # --- Lưu ---
    if st.button("💾 Lưu giao dịch", type="primary", use_container_width=True):
        try:
            if amt <= 0:
                st.error("Số tiền phải lớn hơn 0.")
                st.stop()
            add_transaction(uid, acc_id, ttype, category_id, amt, notes, occurred_dt)
            _toast_ok("✅ Đã thêm giao dịch thành công")
            st.session_state["add_tx_amount"] = ""
        except Exception as e:
            st.error(f"Lưu thất bại. Vui lòng kiểm tra lại dữ liệu. ({e})")

# ---------- Stable KPI cache ----------
def _kpi_cache_get(uid: int, d1: dt.date, d2: dt.date):
    key = f"kpi::{uid}::{str(d1)}::{str(d2)}"
    if "_kpi_cache" not in st.session_state:
        st.session_state._kpi_cache = {}
    return st.session_state._kpi_cache.get(key)

def _kpi_cache_set(uid: int, d1: dt.date, d2: dt.date, val: tuple[float,float,float]):
    key = f"kpi::{uid}::{str(d1)}::{str(d2)}"
    st.session_state._kpi_cache[key] = val

def kpi(uid, d1, d2, mode):
    """
    - Tổng thu/chi/chênh lệch CHỈ phụ thuộc [d1, d2]
    - Chỉ phần 'so với kỳ trước' phụ thuộc 'mode'
    - Có cache theo (uid, d1, d2) để không nhảy số khi re-run
    """
    cached = _kpi_cache_get(uid, d1, d2)
    if cached is None:
        income, expense, net = period_sum(uid, d1, d2)
        _kpi_cache_set(uid, d1, d2, (income, expense, net))
    else:
        income, expense, net = cached

    # Kỳ trước để so sánh (phụ thuộc mode, nhưng KHÔNG ảnh hưởng tổng hiện tại)
    p1, p2 = previous_period(d1, d2, mode)
    pin, pex, pnet = period_sum(uid, p1, p2)

    def fmt_delta(v, pv):
        d = v - pv
        arrow = "↑" if d > 0 else ("↓" if d < 0 else "→")
        return f"{arrow} {format_vnd(abs(d))} VND so với kỳ trước"

    c1, c2, c3 = st.columns(3)
    c1.markdown(
        f"<div style='color:#888'>Tổng thu</div>"
        f"<div style='color:{COLOR_INCOME};font-weight:700;font-size:2.2rem'>{format_vnd(income)} VND</div>"
        f"<div style='color:#888;font-size:0.9rem'>{fmt_delta(income, pin)}</div>", unsafe_allow_html=True
    )
    c2.markdown(
        f"<div style='color:#888'>Tổng chi</div>"
        f"<div style='color:{COLOR_EXPENSE};font-weight:700;font-size:2.2rem'>{format_vnd(expense)} VND</div>"
        f"<div style='color:#888;font-size:0.9rem'>{fmt_delta(expense, pex)}</div>", unsafe_allow_html=True
    )
    c3.markdown(
        f"<div style='color:#888'>Chênh lệch (thu - chi)</div>"
        f"<div style='color:{COLOR_NET};font-weight:700;font-size:2.2rem'>{format_vnd(net)} VND</div>"
        f"<div style='color:#888;font-size:0.9rem'>{fmt_delta(net, pnet)}</div>", unsafe_allow_html=True
    )

def spending_chart(uid, d1, d2, mode, chart_type: str):
    df, label, xtype = query_agg_expense(uid, d1, d2, mode)
    if df.empty:
        st.info("Chưa có dữ liệu."); return
    if chart_type == "Cột":
        mark = alt.Chart(df).mark_bar(color=COLOR_EXPENSE)
    else:
        mark = alt.Chart(df).mark_line(point=True, color=COLOR_EXPENSE)
    ch = mark.encode(
        x=alt.X(f"{label}:{xtype}", title=label),
        y=alt.Y("Chi_tieu:Q", title="Chi tiêu (VND)"),
        tooltip=[label, alt.Tooltip("Chi_tieu:Q", format=",.0f", title="Chi tiêu")]
    ).properties(height=260)
    st.altair_chart(ch, use_container_width=True)

def pie_by_category(uid, d1, d2, group_parent=True):
    if group_parent:
        df = get_df("""
            SELECT COALESCE(cp.name, c.name) AS Danh_mục,
                   SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
            FROM transactions t
            LEFT JOIN categories c  ON c.id=t.category_id
            LEFT JOIN categories cp ON cp.id=c.parent_id
            WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
            GROUP BY COALESCE(cp.name, c.name)
            HAVING Chi_tiêu>0 ORDER BY Chi_tiêu DESC
        """, (uid, str(d1), str(d2)))
    else:
        df = get_df("""
            SELECT COALESCE(c.name,'(Không danh mục)') AS Danh_mục,
                   SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
            FROM transactions t LEFT JOIN categories c ON c.id=t.category_id
            WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
            GROUP BY c.name HAVING Chi_tiêu>0 ORDER BY Chi_tiêu DESC
        """, (uid, str(d1), str(d2)))

    if df.empty:
        st.info("Chưa có chi tiêu theo danh mục."); return

    st.altair_chart(
        alt.Chart(df).mark_arc().encode(
            theta="Chi_tiêu:Q",
            color=alt.Color("Danh_mục:N", legend=None, scale=alt.Scale(scheme="tableau10")),
            tooltip=["Danh_mục", alt.Tooltip("Chi_tiêu:Q", format=",.0f")]
        ).properties(height=260),
        use_container_width=True
    )

# ----------- BUDGETS: % đúng thực, auto-scale, 2 chế độ hiển thị -----------
def budget_progress_df(uid, d1, d2):
    """
    Trả về DataFrame: Danh mục | Đã dùng | Hạn mức | %
    - % KHÔNG bị cắt, hiển thị đúng giá trị thực (có thể > 100, 200, 300%…)
    """
    b = get_df("""SELECT b.id, b.category_id, c.name AS category, b.amount, b.start_date, b.end_date
                  FROM budgets b JOIN categories c ON c.id=b.category_id
                  WHERE b.user_id=? AND date(b.end_date)>=date(?) AND date(b.start_date)<=date(?)
                  ORDER BY b.start_date DESC""", (uid, str(d1), str(d2)))
    if b.empty: 
        return b
    rows=[]
    for _,r in b.iterrows():
        s = max(pd.to_datetime(str(r["start_date"])).date(), d1)
        e = min(pd.to_datetime(str(r["end_date"])).date(), d2)
        spent = fetchone("""SELECT COALESCE(SUM(amount),0) s FROM transactions
                            WHERE user_id=? AND type='expense' AND category_id=?
                              AND date(occurred_at) BETWEEN date(?) AND date(?)""",
                         (uid, int(r["category_id"]), str(s), str(e)))
        used = float(spent["s"] or 0.0)
        limit = float(r["amount"])
        pct = 0.0 if limit<=0 else (100.0*used/limit)   # <-- KHÔNG CLIP
        rows.append({"Danh mục": r["category"], "Đã dùng": used, "Hạn mức": limit, "%": pct})
    return pd.DataFrame(rows)

def budget_progress_chart(df, title: str = "Tiến độ hạn mức"):
    """
    Vẽ bar ngang với trục X tự co giãn theo % lớn nhất.
    Màu: <90% xanh, 90–100% vàng, >100% đỏ.
    """
    if df is None or df.empty:
        st.info("Chưa có hạn mức.")
        return

    d = df.copy()
    d["%"] = pd.to_numeric(d["%"], errors="coerce").fillna(0.0)

    # domain trục X: làm tròn lên bội 10 để nhìn đẹp
    max_pct = max(100.0, float(d["%"].max()))
    domain_right = int(math.ceil(max_pct / 10.0) * 10)

    def pct_to_color(p):
        p = float(p)
        if p < 90:
            return "#22c55e"   # xanh
        if p <= 100:
            return "#f59e0b"   # vàng
        return "#ef4444"       # đỏ

    d["__color"] = [pct_to_color(x) for x in d["%"]]

    base = alt.Chart(d).encode(
        y=alt.Y("Danh mục:N", sort='-x', title=None)
    )

    bars = base.mark_bar().encode(
        x=alt.X("%:Q", title="Đã dùng (%)", scale=alt.Scale(domain=[0, domain_right])),
        color=alt.Color("__color:N", legend=None, scale=None),
        tooltip=[
            alt.Tooltip("Danh mục:N"),
            alt.Tooltip("%:Q", format=".0f", title="Đã dùng (%)"),
            alt.Tooltip("Đã dùng:Q", format=",.0f"),
            alt.Tooltip("Hạn mức:Q", format=",.0f"),
        ],
    )

    labels = base.mark_text(align="left", dx=4).encode(
        x=alt.X("%:Q", scale=alt.Scale(domain=[0, domain_right])),
        text=alt.Text("%:Q", format=".0f")
    )

    st.markdown(f"#### {title}")
    st.altair_chart((bars + labels).properties(height=max(220, 28*len(d))), use_container_width=True)

    # Banner cảnh báo
    over = d[d["%"] > 100]
    if not over.empty:
        items = [
            f"{r['Danh mục']} ({r['%']:.0f}% | {format_vnd(r['Đã dùng'])}/{format_vnd(r['Hạn mức'])})"
            for _, r in over.iterrows()
        ]
        st.warning("⚠ Danh mục vượt hạn mức: " + " · ".join(items))

# ----------------- HOME -----------------
def page_home(uid):
    st.subheader("🏠 Trang chủ")

    today = dt.date.today()
    # Giữ trạng thái bộ lọc ngày
    if "filter_start" not in st.session_state:
        st.session_state.filter_start = today.replace(day=1)
    if "filter_end" not in st.session_state:
        st.session_state.filter_end = today

    # Hàng chọn ngày
    c1, c2 = st.columns(2)
    st.session_state.filter_start = c1.date_input("Từ ngày", st.session_state.filter_start)
    st.session_state.filter_end   = c2.date_input("Đến ngày", st.session_state.filter_end)

    # Snapshot khoảng ngày dùng thống nhất toàn trang
    cur_start = st.session_state.filter_start
    cur_end   = st.session_state.filter_end

    # Lưu chế độ hiển thị trong session để KPI không “nhấp nháy” số
    if "home_mode" not in st.session_state:
        st.session_state.home_mode = "day"  # day/week/month/year

    # KPI (tổng thu/chi/net đặt ngay dưới bộ chọn ngày)
    kpi(uid, cur_start, cur_end, st.session_state.home_mode)

    st.divider()

    # Hàng điều khiển: Chế độ hiển thị & Kiểu biểu đồ (đưa lên trước biểu đồ)
    ctl1, ctl2, _ = st.columns([1.2, 1.0, 2])
    mode = ctl1.radio("Chế độ hiển thị", ["Ngày","Tuần","Tháng","Năm"],
                      horizontal=True,
                      index=["day","week","month","year"].index(st.session_state.home_mode))
    mode_key = {"Ngày":"day","Tuần":"week","Tháng":"month","Năm":"year"}[mode]
    st.session_state.home_mode = mode_key  # cập nhật cho lần render kế tiếp

    chart_type = ctl2.radio("Kiểu biểu đồ", ["Cột","Đường"], horizontal=True, index=0)

    # Điều chỉnh khoảng cho CHART (riêng biểu đồ để dễ nhìn gọn)
    chart_d1, chart_d2 = cur_start, cur_end
    if mode_key == "week":
        chart_d1 = start_weeks_back(chart_d2, 12)
    elif mode_key == "month":
        chart_d1 = start_months_back(chart_d2, 12)
    elif mode_key == "year":
        chart_d1, chart_d2 = year_window(chart_d2, 5)

    colA, colB = st.columns([2, 1])
    with colA:
        st.markdown(f"#### Biểu đồ theo {mode.lower()}")
        spending_chart(uid, chart_d1, chart_d2, mode_key, chart_type)
        st.caption(f"Khoảng hiển thị: {chart_d1} → {chart_d2}")

    with colB:
        st.markdown("#### Cơ cấu theo danh mục")
        # Mặc định gộp theo danh mục cha = True
        group_parent = st.toggle("Gộp theo danh mục cha", value=True, key="home_group_parent")
        pie_by_category(uid, cur_start, cur_end, group_parent)

    st.divider()
    st.markdown("#### Tiến độ hạn mức")
    dfb = budget_progress_df(uid, cur_start, cur_end)
    # Trang chủ: chỉ hiện các hạn mức sắp chạm/vượt (>= 90%)
    near_threshold = 90.0
    df_alert = dfb[dfb["%"] >= near_threshold] if dfb is not None and not dfb.empty else dfb
    if df_alert is None or df_alert.empty:
        st.success("🎉 Chưa có danh mục nào gần chạm hoặc vượt hạn mức.")
    else:
        budget_progress_chart(df_alert, title="Tiến độ hạn mức (gần chạm/vượt)")

    st.divider()
    st.markdown("#### Giao dịch gần đây")
    df = df_tx_vi(list_transactions(uid, today - dt.timedelta(days=7), today))
    if df is None or df.empty:
        st.info("Chưa có giao dịch tuần này.")
    else:
        if "Loại" in df.columns:
            df["Loại"] = df["Loại"].map({"Thu nhập":"🟢 Thu nhập","Chi tiêu":"🔴 Chi tiêu"}).fillna(df["Loại"])
        df = df.drop(columns=[c for c in df.columns if c in META_DROP], errors="ignore")
        df.insert(0, "STT", range(1, len(df)+1))
        st.dataframe(df.head(10), use_container_width=True, height=260, hide_index=True)

def current_balance(uid, account_id):
    r = fetchone("""SELECT
      (SELECT opening_balance FROM accounts WHERE id=? AND user_id=?) +
      COALESCE((SELECT SUM(amount) FROM transactions WHERE user_id=? AND account_id=? AND type='income'),0) -\
      COALESCE((SELECT SUM(amount) FROM transactions WHERE user_id=? AND account_id=? AND type='expense'),0)
      AS bal""", (account_id,uid,uid,account_id,uid,account_id))
    return float(r["bal"] or 0.0)

def page_accounts(uid):
    render_inline_notice()

    st.subheader("👛 Ví / Tài khoản")
    df = get_accounts(uid)
    if df.empty:
        st.info("Chưa có ví nào.")
    else:
        disp = df.copy()
        disp["Tên"]  = disp["name"]
        disp["Loại"] = disp["type"].map({"cash":"Tiền mặt","bank":"Tài khoản ngân hàng","card":"Thẻ"})
        disp["Tiền tệ"] = disp["currency"]
        disp["Số dư hiện tại"] = [format_vnd(current_balance(uid, int(r["id"]))) for _, r in df.iterrows()]
        disp = disp[["Tên","Loại","Tiền tệ","Số dư hiện tại"]]

        render_table(
            disp,
            default_sort_col="Số dư hiện tại",
            default_asc=False,
            height=320,
            key_suffix="accounts",
            exclude_sort_cols={"Tên","Loại","Tiền tệ"},
            show_type_filters=False,
            show_sort=True
        )

    st.markdown("#### Thêm ví mới")
    name = st.text_input("Tên ví (tuỳ chọn)")
    ttype = st.selectbox("Loại",["cash","bank","card"],
                         format_func=lambda x: {"cash":"Tiền mặt","bank":"Tài khoản ngân hàng","card":"Thẻ"}[x])
    opening = money_input("Số dư ban đầu (VND)", key="open_balance", placeholder="VD: 2.000.000")
    if st.button("Thêm ví", type="primary"):
        add_account(uid, name or {"cash":"Tiền mặt","bank":"Tài khoản ngân hàng","card":"Thẻ"}[ttype], ttype, opening)
        _toast_ok("✅ Đã thêm ví mới!")
        st.rerun()

def page_categories(uid):
    render_inline_notice()

    st.subheader("🏷️ Danh mục")

    tab_exp, tab_inc = st.tabs(["Chi tiêu","Thu nhập"])
    for ctype_vi, tab in [("Chi tiêu", tab_exp), ("Thu nhập", tab_inc)]:
        ctype = "expense" if ctype_vi=="Chi tiêu" else "income"
        with tab:
            parents, _ = build_category_tree(uid, ctype)
            if not parents:
                st.info("Chưa có danh mục.")
            else:
                for p in parents:
                    with st.expander(f"🏷️ {p['name']}"):
                        children = p.get("children", [])
                        if not children:
                            st.caption("— (Chưa có danh mục con)")
                        else:
                            for ch in children:
                                st.markdown(f"- 🏷️ **{ch['name']}**")

            st.markdown("##### Thêm danh mục")
            cname = st.text_input(f"Tên danh mục ({ctype_vi})", key=f"cat_name_{ctype}")
            # chọn cha (có thể để (Không))
            all_parents_df = get_df("SELECT id,name FROM categories WHERE user_id=? AND type=? AND parent_id IS NULL ORDER BY name",
                                    (uid, ctype))
            parent_names = ["(Không)"] + all_parents_df["name"].tolist()
            parent_pick = st.selectbox("Thuộc danh mục cha (tuỳ chọn)", parent_names, key=f"cat_parent_{ctype}")
            parent_id = None
            if parent_pick != "(Không)":
                parent_id = int(all_parents_df.loc[all_parents_df["name"]==parent_pick, "id"].iloc[0])

            ccol1, ccol2 = st.columns([1,1])
            if ccol1.button("Thêm danh mục", key=f"btn_add_cat_{ctype}"):
                if cname.strip():
                    add_category(uid, cname.strip(), ctype, parent_id)
                    _toast_ok("✅ Đã thêm danh mục!")
                    st.rerun()
                else:
                    show_notice("❌ Tên danh mục không được để trống.", "error"); st.rerun()

            with ccol2.popover("🗑️ Xoá danh mục", use_container_width=True):
                all_cats = get_df("SELECT id,name FROM categories WHERE user_id=? AND type=? ORDER BY name",(uid,ctype))
                if all_cats.empty:
                    st.caption("Chưa có danh mục để xoá.")
                else:
                    del_name = st.selectbox("Chọn danh mục", all_cats["name"].tolist(), key=f"del_{ctype}")
                    del_id = int(all_cats.loc[all_cats["name"]==del_name, "id"].iloc[0])
                    st.caption("• Xoá sẽ: xoá budgets liên quan, set NULL cho giao dịch thuộc danh mục này, bỏ liên kết cha của các danh mục con.")
                    if st.button("Xác nhận xoá", type="secondary", key=f"do_del_{ctype}"):
                        delete_category(uid, del_id)
                        _toast_ok("🗑️ Đã xoá danh mục.")
                        st.rerun()

def page_budgets(uid):
    render_inline_notice()

    st.subheader("🎯 Ngân sách")
    st.caption("Đặt hạn mức chi tiêu theo khoảng ngày cho từng danh mục Chi tiêu.")

    cats = get_categories(uid, "expense")
    if cats.empty:
        st.info("Chưa có danh mục Chi tiêu."); return

    cat = st.selectbox("Danh mục", cats["name"])
    cat_id = int(cats[cats["name"]==cat]["id"].iloc[0])
    start = st.date_input("Từ ngày", value=dt.date.today().replace(day=1))
    end   = st.date_input("Đến ngày", value=dt.date.today())
    amount = money_input("Hạn mức (VND)", key="budget_amount", placeholder="VD: 2.500.000")

    bcol1, bcol2 = st.columns([1,1])
    if bcol1.button("Lưu hạn mức", type="primary"):
        execute("""INSERT INTO budgets(user_id,category_id,amount,start_date,end_date)
                   VALUES(?,?,?,?,?)""", (uid, cat_id, float(amount), str(start), str(end)))
        _toast_ok("✅ Đã lưu hạn mức!")
        st.rerun()

    with bcol2.popover("🗑️ Xoá hạn mức", use_container_width=True):
        dfb = get_df("""SELECT b.id, c.name AS category, b.start_date, b.end_date, b.amount
                        FROM budgets b JOIN categories c ON c.id=b.category_id
                        WHERE b.user_id=? ORDER BY b.start_date DESC""", (uid,))
        if dfb.empty:
            st.caption("Chưa có hạn mức để xoá.")
        else:
            pick = st.selectbox("Chọn hạn mức", [f"{r['category']} ({r['start_date']} → {r['end_date']}) - {format_vnd(r['amount'])} VND" for _,r in dfb.iterrows()])
            sel_idx = [f"{r['category']} ({r['start_date']} → {r['end_date']}) - {format_vnd(r['amount'])} VND" for _,r in dfb.iterrows()].index(pick)
            bid = int(dfb.iloc[sel_idx]["id"])
            if st.button("Xác nhận xoá", type="secondary"):
                delete_budget(uid, bid)
                _toast_ok("🗑️ Đã xoá hạn mức.")
                st.rerun()

    st.divider()
    st.markdown("#### Hạn mức hiện có")
    df = get_df("""SELECT b.id, c.name AS category, b.amount, b.start_date, b.end_date
                   FROM budgets b JOIN categories c ON c.id=b.category_id
                   WHERE b.user_id=? ORDER BY b.start_date DESC""", (uid,))
    if df.empty:
        st.info("Chưa có hạn mức.")
    else:
        df = df.rename(columns={"category":"Danh mục","amount":"Hạn mức (VND)","start_date":"Từ ngày","end_date":"Đến ngày"})
        df["Hạn mức (VND)"] = df["Hạn mức (VND)"].map(format_vnd)
        render_table(df, default_sort_col="Từ ngày", default_asc=False, height=260, key_suffix="budgets",
                     exclude_sort_cols=set())

    # Biểu đồ FULL tất cả hạn mức (lấy khoảng ngày chung nếu có, mặc định tháng này -> hôm nay)
    st.divider()
    chart_start = st.session_state.get("filter_start", dt.date.today().replace(day=1))
    chart_end   = st.session_state.get("filter_end", dt.date.today())
    df_all = budget_progress_df(uid, chart_start, chart_end)
    budget_progress_chart(df_all, title="Tiến độ hạn mức (tất cả)")

def page_reports(uid):
    render_inline_notice()

    st.subheader("📈 Báo cáo")

    today = dt.date.today()
    default_start = st.session_state.get("filter_start", today.replace(day=1))
    default_end   = st.session_state.get("filter_end", today)
    date_range = st.date_input("Theo ngày", value=(default_start, default_end))
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start, end = date_range
    else:
        start, end = default_start, default_end
    st.session_state.filter_start, st.session_state.filter_end = start, end

    st.markdown("#### Top danh mục chi")
    group_parent = st.toggle("Gộp theo danh mục cha", value=True, key="rep_group_parent")
    if group_parent:
        df = get_df("""
            SELECT COALESCE(cp.name, c.name) AS Danh_mục,
                   SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
            FROM transactions t
            LEFT JOIN categories c  ON c.id=t.category_id
            LEFT JOIN categories cp ON cp.id=c.parent_id
            WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
            GROUP BY COALESCE(cp.name, c.name)
            HAVING Chi_tiêu>0 ORDER BY Chi_tiêu DESC LIMIT 10
        """, (uid, str(start), str(end)))
    else:
        df = get_df("""
            SELECT COALESCE(c.name,'(Không danh mục)') AS Danh_mục,
                   SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
            FROM transactions t LEFT JOIN categories c ON c.id=t.category_id
            WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
            GROUP BY c.name HAVING Chi_tiêu>0 ORDER BY Chi_tiêu DESC LIMIT 10
        """, (uid, str(start), str(end)))

    if df.empty:
        st.info("Chưa có dữ liệu.")
    else:
        st.altair_chart(
            alt.Chart(df).mark_bar().encode(
                x=alt.X("Chi_tiêu:Q", title="Chi tiêu (VND)"),
                y=alt.Y("Danh_mục:N", sort='-x', title="Danh mục"),
                color=alt.Color("Danh_mục:N", legend=None, scale=alt.Scale(scheme="tableau10")),
                tooltip=["Danh_mục", alt.Tooltip("Chi_tiêu:Q", format=",.0f")]
            ).properties(height=320),
            use_container_width=True
        )

    st.markdown("#### 📊 Danh sách giao dịch")
    raw_df = list_transactions(uid, start, end)
    df = df_tx_vi(raw_df)
    if df is not None and not df.empty and "Loại" in df.columns:
        df["Loại"] = df["Loại"].map({"Thu nhập":"🟢 Thu nhập","Chi tiêu":"🔴 Chi tiêu"}).fillna(df["Loại"])
    render_table(df, default_sort_col="Thời điểm", default_asc=False, height=380,
                 key_suffix="report_tx", exclude_sort_cols={"Loại","Tiền tệ"},
                 show_type_filters=True, show_sort=True)

    st.divider()
    st.markdown("#### 📥 Xuất dữ liệu")
    if df is None or df.empty:
        st.caption("Không có dữ liệu để xuất.")
    else:
        export = raw_df.rename(columns={
            "occurred_at":"Ngày giao dịch",
            "account":"Ví / Tài khoản",
            "category":"Danh mục",
            "amount":"Số tiền (VND)",
            "currency":"Tiền tệ",
            "notes":"Ghi chú",
            "tags":"Thẻ",
            "merchant":"Nơi chi tiêu"
        })
        order = ["Ngày giao dịch","Ví / Tài khoản","Danh mục","Số tiền (VND)","Tiền tệ","Ghi chú","Thẻ","Nơi chi tiêu"]
        export = export[[c for c in order if c in export.columns]]

        csv_bytes = export.to_csv(index=False).encode("utf-8-sig")
        st.download_button("Tải transactions.csv", csv_bytes, file_name="transactions.csv", mime="text/csv")

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            export.to_excel(writer, index=False, sheet_name="transactions")
            wb = writer.book
            ws = writer.sheets["transactions"]

            fmt_header = wb.add_format({
                "bold": True, "align": "center", "valign": "vcenter",
                "bg_color": "#EEEEEE", "border": 1
            })
            fmt_center = wb.add_format({"align": "center", "valign": "vcenter"})
            fmt_left   = wb.add_format({"align": "left", "valign": "vcenter"})
            fmt_money  = wb.add_format({"num_format": "#,##0", "align": "center", "valign": "vcenter"})
            fmt_datetime = wb.add_format({"num_format": "yyyy-mm-dd hh:mm", "align": "center", "valign": "vcenter"})

            for col_idx, col_name in enumerate(export.columns):
                ws.write(0, col_idx, col_name, fmt_header)

            for i, col in enumerate(export.columns):
                width = max(12, min(40, int(export[col].astype(str).str.len().quantile(0.9)) + 2))
                if "Số tiền" in col:
                    ws.set_column(i, i, width, fmt_money)
                elif "Ngày giao dịch" in col:
                    ws.set_column(i, i, width, fmt_datetime)
                elif col in ("Ghi chú","Thẻ","Nơi chi tiêu"):
                    ws.set_column(i, i, width, fmt_left)
                else:
                    ws.set_column(i, i, width, fmt_center)

            ws.freeze_panes(1, 0)

        st.download_button("Tải transactions.xlsx", buf.getvalue(),
                           file_name="transactions.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

def page_about(uid):
    render_inline_notice()

    st.subheader("ℹ️ Giới thiệu")
    st.markdown(
        """
**Expense Manager** là ứng dụng quản lý thu chi cá nhân viết bằng **Python + Streamlit**, lưu trữ bằng **SQLite**.

### Tính năng nổi bật
- **Theo dõi thu/chi** theo **ngày–tuần–tháng–năm** với **KPI** và so sánh **kỳ trước**
- **Biểu đồ** chi tiêu dạng **Cột/Đường**; **Pie chart** theo danh mục (mặc định **gộp theo danh mục cha**)
- **Danh mục cha–con** (tạo/sửa/xoá); thêm giao dịch theo **cha** hoặc **con**
- **Hạn mức (Ngân sách) theo danh mục**: đặt mục tiêu, xem tiến độ, cảnh báo vượt mức
- **Báo cáo**: Top danh mục chi, danh sách giao dịch, **xuất CSV/XLSX**
- **Quản lý ví/tài khoản** và tính **số dư hiện tại**
- **Xoá an toàn**: giao dịch, hạn mức, danh mục (tự xử lý ràng buộc liên quan)

> Mặc định mọi thống kê đều **gộp theo danh mục cha**; bạn có thể tắt để xem theo danh mục con khi cần.
        """
    )

# ---------- Onboarding ----------
def onboarding_wizard(uid):
    render_inline_notice()

    st.title("🚀 Thiết lập lần đầu")
    if "ob_step" not in st.session_state: st.session_state.ob_step = 1

    if st.session_state.ob_step == 1:
        name = st.text_input("Tên hiển thị của bạn", "")
        if st.button("Tiếp tục ➜", type="primary", disabled=(not name.strip())):
            set_user_profile(uid, name.strip()); st.session_state.ob_step = 2; st.rerun()

    elif st.session_state.ob_step == 2:
        st.write("Nhập số dư ban đầu cho ví (**số tiền thực tế bạn đang có**):")
        df = get_accounts(uid)
        try:
            cash_id = int(df[df["type"]=="cash"]["id"].iloc[0])
            bank_id = int(df[df["type"]=="bank"]["id"].iloc[0])
        except Exception:
            st.error("Không tìm thấy ví mặc định. Hãy đăng xuất và đăng ký lại."); return
        c1,c2 = st.columns(2)
        cash_text = c1.text_input("Tiền mặt (VND)", placeholder="VD: 2.000.000", key="ob_cash")
        bank_text = c2.text_input("Tài khoản ngân hàng (VND)", placeholder="VD: 8.000.000", key="ob_bank")
        if st.button("Lưu & tiếp tục ➜", type="primary"):
            execute("UPDATE accounts SET opening_balance=? WHERE id=?", (float(parse_vnd_str(cash_text)), cash_id))
            execute("UPDATE accounts SET opening_balance=? WHERE id=?", (float(parse_vnd_str(bank_text)), bank_id))
            st.session_state.ob_step = 3; st.rerun()

    else:
        st.write("Tạo **ít nhất một danh mục Chi tiêu** và **một danh mục Thu nhập**.")
        cats_all = get_categories(uid)
        col = st.columns(2)
        with col[0]:
            cname_e = st.text_input("Tên danh mục Chi tiêu", key="ob_e")
            if st.button("Thêm danh mục Chi tiêu"):
                if cname_e.strip(): add_category(uid, cname_e.strip(), "expense"); st.rerun()
        with col[1]:
            cname_i = st.text_input("Tên danh mục Thu nhập", key="ob_i")
            if st.button("Thêm danh mục Thu nhập"):
                if cname_i.strip(): add_category(uid, cname_i.strip(), "income"); st.rerun()

        if not cats_all.empty:
            show = cats_all.rename(columns={"name":"Tên","type":"Loại"})[["Tên","Loại"]]
            render_table(show, height=220, key_suffix="ob", show_type_filters=False, show_sort=False)

        ok = (not get_categories(uid, "expense").empty) and (not get_categories(uid, "income").empty)
        if st.button("Hoàn tất", type="primary", disabled=(not ok)):
            finish_onboarding(uid); st.success("Xong! Bắt đầu dùng ứng dụng thôi 🎉"); st.rerun()

# ---------- Shell ----------
def screen_login():
    st.title("💸 Expense Manager")
    st.caption("Quản lý chi tiêu cá nhân — Streamlit + SQLite")

    render_inline_notice()

    tab1, tab2 = st.tabs(["Đăng nhập","Đăng ký"])

    with tab1:
        email = st.text_input("Email")
        pw    = st.text_input("Mật khẩu", type="password")
        if st.button("Đăng nhập", type="primary", use_container_width=True):
            uid = login_user(email, pw)
            if uid:
                st.session_state.user_id = int(uid)
                _toast_ok("✅ Đăng nhập thành công")
                st.rerun()
            else:
                show_notice("❌ Sai email hoặc mật khẩu.", "error"); st.rerun()

    with tab2:
        email_r = st.text_input("Email đăng ký")
        pw1 = st.text_input("Mật khẩu", type="password", key="pw1")
        pw2 = st.text_input("Nhập lại mật khẩu", type="password", key="pw2")
        if st.button("Tạo tài khoản", use_container_width=True):
            if not email_r or not pw1:
                show_notice("❌ Vui lòng điền đầy đủ thông tin.", "error"); st.rerun()
            elif pw1 != pw2:
                show_notice("❌ Mật khẩu nhập lại không khớp.", "error"); st.rerun()
            else:
                ok, msg = create_user(email_r, pw1)
                if ok:
                    _toast_ok(msg)
                    st.rerun()
                else:
                    show_notice(msg, "error"); st.rerun()

def app_shell(uid: int):
    u = get_user(uid)
    with st.sidebar:
        st.markdown("### 💶 Expense Manager")
        st.write(f"👤 **{u['display_name'] or u['email']}**")
        st.caption(dt.date.today().strftime("%d/%m/%Y"))
        nav = st.radio("Điều hướng",
                       ["Trang chủ","Giao dịch","Ví/Tài khoản","Danh mục","Ngân sách","Báo cáo","Giới thiệu"],
                       label_visibility="collapsed", index=0)
        st.session_state.nav = nav
        if st.button("Đăng xuất", use_container_width=True):
            st.session_state.clear()
            _toast_ok("Đã đăng xuất")
            st.rerun()

    if nav == "Trang chủ":      page_home(uid)
    elif nav == "Giao dịch":    page_transactions(uid)
    elif nav == "Ví/Tài khoản": page_accounts(uid)
    elif nav == "Danh mục":     page_categories(uid)
    elif nav == "Ngân sách":    page_budgets(uid)
    elif nav == "Báo cáo":      page_reports(uid)
    else:                       page_about(uid)

# ---------- Main ----------
def main():
    st.set_page_config(page_title="Expense Manager", page_icon="💸", layout="wide")
    init_db()
    if "user_id" not in st.session_state:
        screen_login(); return
    u = get_user(st.session_state.user_id)
    if not u:
        st.session_state.clear(); screen_login(); return
    if int(u["onboarded"] or 0) == 0:
        onboarding_wizard(st.session_state.user_id)
    else:
        app_shell(st.session_state.user_id)

if __name__ == "__main__":
    main()
