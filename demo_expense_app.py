# ==========================================
# Expense Manager (Streamlit + SQLite)
# Bản đã hợp nhất toàn bộ yêu cầu
# ==========================================

import streamlit as st
import sqlite3, hashlib, pandas as pd, datetime as dt, altair as alt
from pathlib import Path
import random, re, unicodedata

DB_PATH = "expense.db"
ENABLE_DEMO = True

# ---------- Helpers tiền tệ / thời gian ----------
def format_vnd(n):
    try:
        return f"{float(n):,.0f}".replace(",", ".")
    except Exception:
        return str(n)

def parse_vnd_str(s):
    if s is None:
        return 0.0
    digits = re.sub(r"[^\d-]", "", str(s))
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

# NEW: khoảng hiển thị cho mode Tháng/Năm
def start_months_back(end_date: dt.date, months: int) -> dt.date:
    idx = end_date.year * 12 + (end_date.month - 1) - (months - 1)
    y0 = idx // 12
    m0 = idx % 12 + 1
    return dt.date(y0, m0, 1)

def year_window(end_date: dt.date, years: int) -> tuple[dt.date, dt.date]:
    y2 = end_date.year
    y1 = y2 - (years - 1)
    return dt.date(y1, 1, 1), dt.date(y2, 12, 31)

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

# ---------- Seed DEMO: đủ 2023, 2024 + các tháng của năm hiện tại, budgets 12 tháng ----------
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
        for _ in range(random.randint(6, 8)):  # incomes
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

    # regenerate demo txs
    c.execute("DELETE FROM transactions WHERE user_id=?", (uid,))
    for y in (2023, 2024):
        for m in range(1, 13):
            add_month_data(y, m)
    today = dt.date.today()
    for m in range(1, today.month + 1):  # năm hiện tại đến tháng hiện tại
        add_month_data(today.year, m)
    c.commit()

    # budgets 12 tháng gần nhất
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
COLOR_NET = "#06b6d4"   # Net dùng 1 màu cố định

def list_transactions(uid, d1=None, d2=None):
    q = """SELECT t.id, t.occurred_at, t.type, t.amount, t.currency,
                  a.name AS account, c.name AS category, t.notes, t.tags, t.merchant_id AS merchant
           FROM transactions t JOIN accounts a ON a.id=t.account_id
           LEFT JOIN categories c ON c.id=t.category_id
           WHERE t.user_id=?"""
    p=[uid]
    if d1: q+=" AND date(t.occurred_at)>=date(?)"; p.append(str(d1))
    if d2: q+=" AND date(t.occurred_at)<=date(?)"; p.append(str(d2))
    q += " ORDER BY t.occurred_at DESC"
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
def add_category(uid,name,t): execute("INSERT INTO categories(user_id,name,type) VALUES(?,?,?)",(uid,name.strip(),t))
def add_account(uid,name,t,balance):
    execute("INSERT INTO accounts(user_id,name,type,opening_balance,created_at) VALUES(?,?,?,?,?)",
            (uid,name.strip(),t,balance,dt.datetime.now().isoformat()))

# ---------- Table helpers (ẩn ID + sort đúng + STT đánh sau sort) ----------
META_DROP = {"id","user_id","parent_id","ID","user_id","parent_id"}

def sort_df_for_display(df, sort_col, ascending):
    if df is None or df.empty or sort_col not in df.columns:
        return df
    s = df[sort_col]
    if sort_col in ("Thời điểm","Từ ngày","Đến ngày"):
        key = pd.to_datetime(s, errors="coerce")
    else:
        norm_name = strip_accents_lower(sort_col)
        if ("tien" in norm_name) or ("du" in norm_name):  # 'Số tiền', 'Số dư ...'
            key = (s.astype(str)
                     .str.replace(".","",regex=False)
                     .str.replace(",","",regex=False)
                     .str.strip()
                     .astype("float64"))
        else:
            key = s.astype(str).map(strip_accents_lower)
    return df.iloc[key.sort_values(ascending=ascending).index]

def render_table(df: pd.DataFrame,
                 default_sort_col: str | None = None,
                 default_asc: bool = False,
                 height: int = 320,
                 key_suffix: str = "",
                 exclude_sort_cols: set[str] | None = None):
    if df is None or df.empty:
        st.info("Chưa có dữ liệu."); return
    df = df.drop(columns=[c for c in df.columns if c in META_DROP], errors="ignore").copy()
    cols = [c for c in df.columns if (exclude_sort_cols is None or c not in exclude_sort_cols)]
    if not cols:
        df.insert(0, "STT", range(1, len(df)+1))
        st.dataframe(df, use_container_width=True, height=height, hide_index=True)
        return
    c1,c2,_ = st.columns([1.6,1,2])
    idx = cols.index(default_sort_col) if default_sort_col in cols else 0
    sort_col = c1.selectbox("Sắp xếp theo", cols, index=idx, key=f"sort_{key_suffix}")
    order = c2.radio("Thứ tự", ["Giảm dần","Tăng dần"], horizontal=True, key=f"order_{key_suffix}")
    df_sorted = sort_df_for_display(df, sort_col, ascending=(order=="Tăng dần"))
    df_sorted.insert(0, "STT", range(1, len(df_sorted)+1))
    st.dataframe(df_sorted, use_container_width=True, height=height, hide_index=True)

# ---------- UI parts ----------
def colored_form_css(kind):
    color = COLOR_EXPENSE if kind=="Chi tiêu" else COLOR_INCOME
    st.markdown(f"""
    <style>
      div[data-testid="stExpander"] > details {{ border: 1px solid {color}55; border-radius: 12px; }}
      div[data-testid="stExpander"] summary:hover {{ background-color: {color}1A; }}
      button[kind="primary"] {{ background-color: {color} !important; border-color: {color} !important; }}
    </style>
    """, unsafe_allow_html=True)

def page_transactions(uid):
    st.subheader("🧾 Giao dịch")
    accounts = get_accounts(uid)
    cats_exp = get_categories(uid, "expense")
    cats_inc = get_categories(uid, "income")

    with st.expander("➕ Thêm giao dịch mới", expanded=True):
        ttype = st.radio("Loại giao dịch", ["Chi tiêu","Thu nhập"], horizontal=True)
        colored_form_css(ttype)
        acc = st.selectbox("Chọn ví/tài khoản", accounts["name"])
        cat = st.selectbox("Chọn danh mục", (cats_exp["name"] if ttype=="Chi tiêu" else cats_inc["name"]))
        amt_text = st.text_input("Số tiền (VND)", placeholder="VD: 20.000.000")
        notes = st.text_input("Ghi chú (tùy chọn)")
        date = st.date_input("Ngày giao dịch", value=dt.date.today())
        time = st.time_input("Giờ giao dịch", value=dt.datetime.now().time().replace(second=0, microsecond=0))
        if st.button("Lưu giao dịch", type="primary", use_container_width=True):
            try:
                amt = parse_vnd_str(amt_text)
                if amt <= 0: st.error("Số tiền phải lớn hơn 0."); st.stop()
                acc_id = int(accounts[accounts["name"]==acc]["id"].iloc[0])
                cats_df = (cats_exp if ttype=="Chi tiêu" else cats_inc)
                cat_id = int(cats_df[cats_df["name"]==cat]["id"].iloc[0])
                add_transaction(uid, acc_id, ("expense" if ttype=="Chi tiêu" else "income"),
                                cat_id, amt, notes, join_date_time(date, time))
                st.success("✅ Giao dịch đã được lưu!")
            except Exception:
                st.error("Vui lòng nhập số tiền hợp lệ (ví dụ: 20.000.000).")

    st.divider()
    st.write("### 📊 Danh sách giao dịch")
    df = df_tx_vi(list_transactions(uid))
    if df is not None and not df.empty and "Loại" in df.columns:
        df["Loại"] = df["Loại"].map({"Thu nhập":"🟢 Thu nhập","Chi tiêu":"🔴 Chi tiêu"}).fillna(df["Loại"])
    render_table(df, default_sort_col="Thời điểm", default_asc=False, height=380, key_suffix="tx_list")

def kpi(uid, d1, d2):
    r = fetchone("""
        SELECT
          COALESCE(SUM(CASE WHEN type='income'  THEN amount END),0) AS income,
          COALESCE(SUM(CASE WHEN type='expense' THEN amount END),0) AS expense
        FROM transactions
        WHERE user_id=? AND date(occurred_at) BETWEEN date(?) AND date(?)""",
        (uid, str(d1), str(d2)))
    income, expense = float(r["income"] or 0), float(r["expense"] or 0)
    net = income - expense

    c1,c2,c3 = st.columns(3)
    c1.markdown(f"<div style='color:#888'>Tổng thu</div>"
                f"<div style='color:{COLOR_INCOME};font-weight:700;font-size:1.6rem'>{format_vnd(income)} VND</div>",
                unsafe_allow_html=True)
    c2.markdown(f"<div style='color:#888'>Tổng chi</div>"
                f"<div style='color:{COLOR_EXPENSE};font-weight:700;font-size:1.6rem'>{format_vnd(expense)} VND</div>",
                unsafe_allow_html=True)
    c3.markdown(f"<div style='color:#888'>Net (thu - chi)</div>"
                f"<div style='color:{COLOR_NET};font-weight:700;font-size:1.6rem'>{format_vnd(net)} VND</div>",
                unsafe_allow_html=True)

def query_agg(uid, d1, d2, mode):
    if mode=="day":
        g="date(occurred_at)"; label="Ngày"; xtype="T"
    elif mode=="month":
        g="strftime('%Y-%m', occurred_at)"; label="Tháng"; xtype="O"
    else:
        g="strftime('%Y', occurred_at)"; label="Năm"; xtype="O"
    df = get_df(f"""SELECT {g} AS label,
                           SUM(CASE WHEN type='expense' THEN amount ELSE 0 END) AS Chi_tiêu
                    FROM transactions
                    WHERE user_id=? AND date(occurred_at) BETWEEN date(?) AND date(?)
                    GROUP BY {g} ORDER BY {g}""", (uid, str(d1), str(d2)))
    df = df.rename(columns={"label": label})
    return df, label, xtype

def spending_chart(uid, d1, d2, mode):
    df, label, xtype = query_agg(uid, d1, d2, mode)
    if df.empty:
        st.info("Chưa có dữ liệu."); return
    ch = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X(f"{label}:{xtype}", title=label),
        y=alt.Y("Chi_tiêu:Q", title="Chi tiêu (VND)"),
        tooltip=[label,"Chi_tiêu"]
    ).properties(height=260)
    st.altair_chart(ch, use_container_width=True)

def pie_by_category(uid, d1, d2):
    df = get_df("""SELECT COALESCE(c.name,'(Không danh mục)') AS Danh_mục,
                          SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
                   FROM transactions t LEFT JOIN categories c ON c.id=t.category_id
                   WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
                   GROUP BY c.name HAVING Chi_tiêu>0 ORDER BY Chi_tiêu DESC""",
                 (uid, str(d1), str(d2)))
    if df.empty:
        st.info("Chưa có chi tiêu theo danh mục."); return
    st.altair_chart(
        alt.Chart(df).mark_arc().encode(
            theta="Chi_tiêu:Q", color="Danh_mục:N", tooltip=["Danh_mục","Chi_tiêu"]
        ).properties(height=260),
        use_container_width=True
    )

def budget_progress_df(uid, d1, d2):
    b = get_df("""SELECT b.id, b.category_id, c.name AS category, b.amount, b.start_date, b.end_date
                  FROM budgets b JOIN categories c ON c.id=b.category_id
                  WHERE b.user_id=? AND date(b.end_date)>=date(?) AND date(b.start_date)<=date(?)
                  ORDER BY b.start_date DESC""", (uid, str(d1), str(d2)))
    if b.empty: return b
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
        pct = 0.0 if limit<=0 else min(100.0*used/limit, 200.0)
        rows.append({"Danh mục": r["category"], "Đã dùng": used, "Hạn mức": limit, "%": pct})
    return pd.DataFrame(rows)

# FIX: clamp 0..100 để không mất thanh khi vượt 100%
def budget_progress_chart(df):
    if df.empty:
        st.info("Chưa có hạn mức."); return
    df = df.copy()
    df["%"] = pd.to_numeric(df["%"], errors="coerce").fillna(0.0)
    def pct_to_color(p):
        p = float(p)
        if p < 70:  return "#22c55e"
        if p < 90:  return "#f59e0b"
        return "#ef4444"
    df["__pct_vis"] = df["%"].clip(0, 100)
    df["__color"]   = [pct_to_color(x) for x in df["%"]]
    base = alt.Chart(df).encode(y=alt.Y("Danh mục:N", sort='-x', title=None))
    bg = base.mark_bar(color="#33333322").encode(
        x=alt.X("value:Q", title="Đã dùng (%)", scale=alt.Scale(domain=[0, 100]))
    ).transform_calculate(value="100")
    bar = base.mark_bar().encode(
        x=alt.X("__pct_vis:Q", title="Đã dùng (%)", scale=alt.Scale(domain=[0, 100])),
        color=alt.Color("__color:N", legend=None, scale=None),
        tooltip=[alt.Tooltip("Danh mục:N"),
                 alt.Tooltip("%:Q", format=".0f", title="Đã dùng (%)"),
                 alt.Tooltip("Đã dùng:Q", format=",.0f"),
                 alt.Tooltip("Hạn mức:Q", format=",.0f")]
    )
    txt = base.mark_text(dy=-8).encode(x=alt.X("__pct_vis:Q"), text=alt.Text("%:Q", format=".0f"))
    st.altair_chart((bg + bar + txt).properties(height=max(220, 28*len(df))), use_container_width=True)

# ---------- Pages ----------
def page_home(uid):
    st.subheader("🏠 Trang chủ")
    today = dt.date.today()
    if "filter_start" not in st.session_state: st.session_state.filter_start = today.replace(day=1)
    if "filter_end"   not in st.session_state: st.session_state.filter_end   = today

    c1,c2 = st.columns(2)
    st.session_state.filter_start = c1.date_input("Từ ngày", st.session_state.filter_start)
    st.session_state.filter_end   = c2.date_input("Đến ngày", st.session_state.filter_end)

    st.divider()
    # KPI giữ nguyên theo khoảng ngày gốc
    kpi(uid, st.session_state.filter_start, st.session_state.filter_end)

    # Chế độ hiển thị line chart
    mode = st.radio("Chế độ hiển thị", ["Ngày","Tháng","Năm"], horizontal=True)
    agg_key = {"Ngày":"day","Tháng":"month","Năm":"year"}[mode]

    # Khoảng riêng cho line chart
    chart_d1, chart_d2 = st.session_state.filter_start, st.session_state.filter_end
    if agg_key == "month":
        chart_d1 = start_months_back(chart_d2, 12)     # 12 tháng gần nhất
    elif agg_key == "year":
        chart_d1, chart_d2 = year_window(chart_d2, 5)  # 5 năm gần nhất

    colA, colB = st.columns([2, 1])
    with colA:
        st.markdown(f"#### Chi theo {mode.lower()}")
        spending_chart(uid, chart_d1, chart_d2, agg_key)
        st.caption(f"Khoảng hiển thị: {chart_d1} → {chart_d2}")
    with colB:
        st.markdown("#### Cơ cấu theo danh mục")
        pie_by_category(uid, st.session_state.filter_start, st.session_state.filter_end)

    st.divider()
    st.markdown("#### Tiến độ hạn mức")
    dfb = budget_progress_df(uid, st.session_state.filter_start, st.session_state.filter_end)
    budget_progress_chart(dfb)

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
      COALESCE((SELECT SUM(amount) FROM transactions WHERE user_id=? AND account_id=? AND type='income'),0) -
      COALESCE((SELECT SUM(amount) FROM transactions WHERE user_id=? AND account_id=? AND type='expense'),0)
      AS bal""", (account_id,uid,uid,account_id,uid,account_id))
    return float(r["bal"] or 0.0)

def page_accounts(uid):
    st.subheader("👛 Ví / Tài khoản")
    df = get_accounts(uid)
    if df.empty:
        st.info("Chưa có ví nào.")
    else:
        disp = df.copy()
        disp["Tên"]  = disp["name"]
        disp["Loại"] = disp["type"].map({"cash":"Tiền mặt","bank":"Tài khoản ngân hàng","card":"Thẻ"})
        disp["Tiền tệ"] = disp["currency"]
        # Ẩn 'Số dư ban đầu' theo yêu cầu
        disp["Số dư hiện tại"] = [format_vnd(current_balance(uid, int(r["id"]))) for _,r in df.iterrows()]
        disp = disp[["Tên","Loại","Tiền tệ","Số dư hiện tại"]]
        render_table(disp, default_sort_col="Số dư hiện tại", default_asc=False, height=320,
                     key_suffix="accounts", exclude_sort_cols={"Tên"})

    st.markdown("#### Thêm ví mới")
    name = st.text_input("Tên ví (tuỳ chọn)")
    ttype = st.selectbox("Loại",["cash","bank","card"], format_func=lambda x:{"cash":"Tiền mặt","bank":"Tài khoản ngân hàng","card":"Thẻ"}[x])
    opening = st.number_input("Số dư ban đầu", min_value=0, step=1000)
    if st.button("Thêm ví", type="primary"):
        add_account(uid, name or {"cash":"Tiền mặt","bank":"Tài khoản ngân hàng","card":"Thẻ"}[ttype], ttype, opening)
        st.rerun()

def page_categories(uid):
    st.subheader("🏷️ Danh mục")
    df = get_categories(uid)
    if df.empty:
        st.info("Chưa có danh mục.")
    else:
        show = df.rename(columns={"name":"Tên","type":"Loại"})
        show["Loại"] = show["Loại"].map(TYPE_LABELS_VN)
        show = show[["Tên","Loại"]]
        render_table(show, default_sort_col="Loại", default_asc=True, height=300,
                     key_suffix="cats", exclude_sort_cols={"Tên"})

    st.markdown("#### Thêm danh mục")
    cname = st.text_input("Tên danh mục")
    ctype = st.selectbox("Loại",["expense","income"], format_func=lambda x: TYPE_LABELS_VN[x])
    if st.button("Thêm danh mục", type="primary"):
        if cname.strip(): add_category(uid, cname.strip(), ctype); st.rerun()
        else: st.error("Tên danh mục không được để trống.")

def page_budgets(uid):
    st.subheader("🎯 Ngân sách")
    st.caption("Đặt hạn mức chi tiêu theo khoảng ngày cho từng danh mục Chi tiêu.")

    cats = get_categories(uid, "expense")
    if cats.empty:
        st.info("Chưa có danh mục Chi tiêu."); return

    cat = st.selectbox("Danh mục", cats["name"])
    cat_id = int(cats[cats["name"]==cat]["id"].iloc[0])
    start = st.date_input("Từ ngày", value=dt.date.today().replace(day=1))
    end   = st.date_input("Đến ngày", value=dt.date.today())
    amount = st.number_input("Hạn mức (VND)", min_value=0, step=100000)

    if st.button("Lưu hạn mức", type="primary"):
        execute("""INSERT INTO budgets(user_id,category_id,amount,start_date,end_date)
                   VALUES(?,?,?,?,?)""", (uid, cat_id, float(amount), str(start), str(end)))
        st.success("Đã lưu hạn mức!")

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
        render_table(df, default_sort_col="Từ ngày", default_asc=False, height=260, key_suffix="budgets")

def page_reports(uid):
    st.subheader("📈 Báo cáo")
    today = dt.date.today()
    start = st.date_input("Từ ngày", st.session_state.get("filter_start", today.replace(day=1)))
    end   = st.date_input("Đến ngày", st.session_state.get("filter_end", today))
    st.session_state.filter_start = start; st.session_state.filter_end = end

    st.markdown("#### Top danh mục chi")
    df = get_df("""SELECT COALESCE(c.name,'(Không danh mục)') AS Danh_mục,
                          SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
                   FROM transactions t LEFT JOIN categories c ON c.id=t.category_id
                   WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
                   GROUP BY c.name HAVING Chi_tiêu>0 ORDER BY Chi_tiêu DESC LIMIT 10""",
                 (uid, str(start), str(end)))
    if df.empty:
        st.info("Chưa có dữ liệu.")
    else:
        st.altair_chart(
            alt.Chart(df).mark_bar().encode(
                x=alt.X("Chi_tiêu:Q", title="Chi tiêu (VND)"),
                y=alt.Y("Danh_mục:N", sort='-x', title="Danh mục"),
                tooltip=["Danh_mục","Chi_tiêu"]
            ).properties(height=320),
            use_container_width=True
        )

    st.markdown("#### Danh sách giao dịch")
    df = df_tx_vi(list_transactions(uid, start, end))
    if df is not None and not df.empty and "Loại" in df.columns:
        df["Loại"] = df["Loại"].map({"Thu nhập":"🟢 Thu nhập","Chi tiêu":"🔴 Chi tiêu"}).fillna(df["Loại"])
    render_table(df, default_sort_col="Thời điểm", default_asc=False, height=360, key_suffix="report_tx")

# ---------- Onboarding ----------
def onboarding_wizard(uid):
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
        cash_text = c1.text_input("Tiền mặt (VND)", placeholder="VD: 2.000.000")
        bank_text = c2.text_input("Tài khoản ngân hàng (VND)", placeholder="VD: 8.000.000")
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
            show = cats_all.rename(columns={"name":"Tên","type":"Loại"})
            show["Loại"] = show["Loại"].map(TYPE_LABELS_VN)
            render_table(show[["Tên","Loại"]], default_sort_col="Loại", default_asc=True, height=220,
                         key_suffix="ob", exclude_sort_cols={"Tên"})

        ok = (not get_categories(uid, "expense").empty) and (not get_categories(uid, "income").empty)
        if st.button("Hoàn tất", type="primary", disabled=(not ok)):
            finish_onboarding(uid); st.success("Xong! Bắt đầu dùng ứng dụng thôi 🎉"); st.rerun()

# ---------- Settings ----------
def page_settings(uid):
    st.subheader("⚙️ Cài đặt / Xuất dữ liệu")
    df = list_transactions(uid)
    if df.empty:
        st.info("Chưa có dữ liệu để tải.")
    else:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Tải transactions.csv", csv, file_name="transactions.csv", mime="text/csv")

# ---------- Shell ----------
def screen_login():
    st.title("💸 Expense Manager")
    st.caption("Quản lý chi tiêu cá nhân — Streamlit + SQLite")

    tab1, tab2 = st.tabs(["Đăng nhập","Đăng ký"])

    with tab1:
        email = st.text_input("Email")
        pw    = st.text_input("Mật khẩu", type="password")
        if st.button("Đăng nhập", type="primary", use_container_width=True):
            uid = login_user(email, pw)
            if uid:
                st.session_state.user_id = int(uid); st.rerun()
            else:
                st.error("Sai email hoặc mật khẩu.")

    with tab2:
        email_r = st.text_input("Email đăng ký")
        pw1 = st.text_input("Mật khẩu", type="password", key="pw1")
        pw2 = st.text_input("Nhập lại mật khẩu", type="password", key="pw2")
        if st.button("Tạo tài khoản", use_container_width=True):
            if not email_r or not pw1:
                st.error("Vui lòng điền đầy đủ thông tin.")
            elif pw1 != pw2:
                st.error("Mật khẩu nhập lại không khớp.")
            else:
                ok, msg = create_user(email_r, pw1)
                st.success(msg) if ok else st.error(msg)

def app_shell(uid: int):
    u = get_user(uid)
    with st.sidebar:
        st.markdown("### 💶 Expense Manager")
        st.write(f"👤 **{u['display_name'] or u['email']}**")
        st.caption(dt.date.today().strftime("%d/%m/%Y"))
        nav = st.radio("Điều hướng",
                       ["Trang chủ","Giao dịch","Ví/Tài khoản","Danh mục","Ngân sách","Báo cáo","Cài đặt"],
                       label_visibility="collapsed", index=0)
        st.session_state.nav = nav
        if st.button("Đăng xuất", use_container_width=True):
            st.session_state.clear(); st.rerun()

    if nav == "Trang chủ":      page_home(uid)
    elif nav == "Giao dịch":    page_transactions(uid)
    elif nav == "Ví/Tài khoản": page_accounts(uid)
    elif nav == "Danh mục":     page_categories(uid)
    elif nav == "Ngân sách":    page_budgets(uid)
    elif nav == "Báo cáo":      page_reports(uid)
    else:                       page_settings(uid)

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
