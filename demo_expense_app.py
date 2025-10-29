# ==========================================
# app.py - Expense Manager (Streamlit + SQLite)
# Bản đầy đủ: giao diện tiếng Việt + trang Hướng dẫn
# (đã thêm: nhập tiền có dấu chấm, lưu cả giờ giao dịch)
# ==========================================

import streamlit as st
import sqlite3
import hashlib
import pandas as pd
import datetime as dt
import altair as alt
from pathlib import Path
from collections import defaultdict
import random
import re

DB_PATH = "expense.db"
ENABLE_DEMO = True  # tạo tài khoản demo cho nhóm dev

# =========================
# Helpers: tiền tệ & thời gian
# =========================
def format_vnd(n: float | int) -> str:
    try:
        return f"{float(n):,.0f}".replace(",", ".")
    except Exception:
        return str(n)

def parse_vnd_str(s: str) -> float:
    """
    Nhận chuỗi nhập tiền kiểu VN (VD: '20.000.000', '1,000,000', '  50000  ')
    -> trả về số (float)
    """
    if s is None:
        return 0.0
    digits = re.sub(r"[^\d]", "", str(s))
    return float(digits) if digits else 0.0

def join_date_time(d: dt.date, t: dt.time) -> str:
    """Ghép ngày + giờ thành chuỗi 'YYYY-MM-DD HH:MM'"""
    return dt.datetime.combine(d, t.replace(second=0, microsecond=0)).strftime("%Y-%m-%d %H:%M")

# =========================
# Database & Helper
# =========================
def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode("utf-8")).hexdigest()

def exec_script(conn, script: str):
    conn.executescript(script)
    conn.commit()

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
# =========================
# Database init & Authentication
# =========================
def init_db():
    Path(DB_PATH).touch(exist_ok=True)
    conn = get_conn()
    exec_script(conn, INIT_SQL)
    if ENABLE_DEMO:
        seed_demo_user_once(conn)
    conn.close()

def get_df(query, params=()):
    conn = get_conn()
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df

def execute(query, params=()):
    conn = get_conn()
    conn.execute(query, params)
    conn.commit()
    conn.close()

def fetchone(query, params=()):
    conn = get_conn()
    row = conn.execute(query, params).fetchone()
    conn.close()
    return row

# ======== Auth ========
def create_user(email, pw):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO users(email,password_hash,created_at,onboarded) VALUES(?,?,?,0)",
            (email.lower(), hash_password(pw), dt.datetime.now().isoformat())
        )
        conn.commit()
        uid = conn.execute("SELECT id FROM users WHERE email=?", (email.lower(),)).fetchone()["id"]
        now = dt.datetime.now().isoformat()
        # tạo sẵn 2 ví mặc định
        conn.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                     (uid, "Tiền mặt", "cash", "VND", 0, now))
        conn.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                     (uid, "Tài khoản ngân hàng", "bank", "VND", 0, now))
        conn.commit()
        return True, "Tạo tài khoản thành công!"
    except sqlite3.IntegrityError:
        return False, "Email đã tồn tại."
    finally:
        conn.close()

def login_user(email, pw):
    row = fetchone("SELECT id,password_hash FROM users WHERE email=?", (email.lower(),))
    if not row:
        return None
    return row["id"] if row["password_hash"] == hash_password(pw) else None

def get_user(uid):
    return fetchone("SELECT * FROM users WHERE id=?", (uid,))

def set_user_profile(uid, display_name):
    execute("UPDATE users SET display_name=? WHERE id=?", (display_name.strip(), uid))

def finish_onboarding(uid):
    execute("UPDATE users SET onboarded=1 WHERE id=?", (uid,))

# ======== Demo user seed ========
def seed_demo_user_once(conn):
    if conn.execute("SELECT 1 FROM users WHERE email='demo@expense.local'").fetchone():
        return
    now = dt.datetime.now().isoformat()
    conn.execute("INSERT INTO users(email,password_hash,created_at,display_name,onboarded) VALUES(?,?,?,?,1)",
                 ("demo@expense.local", hash_password("demo1234"), now, "Tài khoản DEMO"))
    uid = conn.execute("SELECT id FROM users WHERE email='demo@expense.local'").fetchone()["id"]

    # tạo ví
    conn.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                 (uid, "Tiền mặt", "cash", "VND", 2000000, now))
    conn.execute("INSERT INTO accounts(user_id,name,type,currency,opening_balance,created_at) VALUES(?,?,?,?,?,?)",
                 (uid, "Tài khoản ngân hàng", "bank", "VND", 8000000, now))

    # tạo danh mục
    cats = [("Ăn uống", "expense"), ("Cà phê", "expense"), ("Giải trí", "expense"), ("Lương", "income")]
    for n, t in cats:
        conn.execute("INSERT INTO categories(user_id,name,type) VALUES(?,?,?)", (uid, n, t))

    # tạo 1 số giao dịch mẫu (ngày ngẫu nhiên, không cần giờ)
    accs = conn.execute("SELECT id FROM accounts WHERE user_id=?", (uid,)).fetchall()
    cat_exp = conn.execute("SELECT id FROM categories WHERE user_id=? AND type='expense'", (uid,)).fetchall()
    cat_inc = conn.execute("SELECT id FROM categories WHERE user_id=? AND type='income'", (uid,)).fetchall()

    for _ in range(25):
        ttype = random.choice(["expense", "income"])
        amt = random.randint(100000, 2000000)
        acc_id = random.choice(accs)["id"]
        cat_id = random.choice(cat_exp if ttype == "expense" else cat_inc)["id"]
        date = dt.date.today() - dt.timedelta(days=random.randint(0, 20))
        conn.execute("""
            INSERT INTO transactions(user_id,account_id,type,category_id,amount,currency,occurred_at,created_at)
            VALUES(?,?,?,?,?,?,?,?)""",
            (uid, acc_id, ttype, cat_id, amt, "VND", str(date), now))
    conn.commit()

# =========================
# Transaction & Category utilities
# =========================
TYPE_LABELS_VN = {"expense": "Chi tiêu", "income": "Thu nhập"}
ACCOUNT_TYPE_LABEL_VN = {"cash": "Tiền mặt", "bank": "Tài khoản ngân hàng", "card": "Thẻ"}

def list_transactions(uid, d1=None, d2=None, cat_id=None):
    q = """SELECT t.id, t.occurred_at, t.type, t.amount, t.currency,
                  a.name AS account, c.name AS category, t.notes, t.tags, t.merchant_id AS merchant
           FROM transactions t JOIN accounts a ON a.id=t.account_id
           LEFT JOIN categories c ON c.id=t.category_id
           WHERE t.user_id=?"""
    params = [uid]
    if d1:
        q += " AND date(t.occurred_at)>=date(?)"
        params.append(str(d1))
    if d2:
        q += " AND date(t.occurred_at)<=date(?)"
        params.append(str(d2))
    if cat_id:
        q += " AND t.category_id=?"
        params.append(cat_id)
    q += " ORDER BY t.occurred_at DESC"
    return get_df(q, tuple(params))

def df_tx_vi(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    m_cols = {
        "id": "ID",
        "occurred_at": "Thời điểm",
        "type": "Loại",
        "amount": "Số tiền",
        "currency": "Tiền tệ",
        "account": "Ví / Tài khoản",
        "category": "Danh mục",
        "notes": "Ghi chú",
        "tags": "Thẻ",
        "merchant": "Nơi chi tiêu"
    }
    df = df.rename(columns={k: v for k, v in m_cols.items() if k in df.columns}).copy()
    if "Loại" in df.columns:
        df["Loại"] = df["Loại"].map({"expense": "Chi tiêu", "income": "Thu nhập"}).fillna(df["Loại"])
    if "Số tiền" in df.columns:
        df["Số tiền"] = df["Số tiền"].map(lambda x: format_vnd(x))
    return df

def get_accounts(uid):
    return get_df("SELECT * FROM accounts WHERE user_id=?", (uid,))

def get_categories(uid, ttype=None):
    q = "SELECT * FROM categories WHERE user_id=?"
    p = [uid]
    if ttype:
        q += " AND type=?"
        p.append(ttype)
    q += " ORDER BY name"
    return get_df(q, tuple(p))

def add_transaction(uid, account_id, ttype, category_id, amount, notes, occurred_dt: str):
    now = dt.datetime.now().isoformat()
    execute("""
        INSERT INTO transactions(user_id,account_id,type,category_id,amount,currency,occurred_at,created_at)
        VALUES(?,?,?,?,?,?,?,?)""",
        (uid, account_id, ttype, category_id, amount, "VND", occurred_dt, now)
    )

def add_category(uid, name, ttype):
    execute("INSERT INTO categories(user_id,name,type) VALUES(?,?,?)", (uid, name.strip(), ttype))

def add_account(uid, name, ttype, balance):
    execute("INSERT INTO accounts(user_id,name,type,opening_balance,created_at) VALUES(?,?,?,?,?)",
            (uid, name.strip(), ttype, balance, dt.datetime.now().isoformat()))

# =========================
# Trang Hướng dẫn & Giao dịch
# =========================
def page_help(uid):
    st.subheader("📘 Hướng dẫn sử dụng")
    st.markdown("""
### 🎯 Giới thiệu
Ứng dụng giúp bạn theo dõi **thu/chi, ví/tài khoản** và **ngân sách hàng tháng**.

### 🪜 Các bước bắt đầu
1. **Đăng ký tài khoản mới** để bắt đầu sử dụng.
2. Làm theo **3 bước thiết lập**:  
   - Nhập **tên hiển thị**  
   - Nhập **số dư ban đầu** cho *Tiền mặt* và *Tài khoản ngân hàng*  
   - Tạo **ít nhất một danh mục Chi tiêu** và **một danh mục Thu nhập**
> *Lưu ý: Tài khoản demo chỉ dành cho team phát triển.*

### 💰 Trang chủ
- Hiển thị **Tổng thu / Tổng chi / Net**  
- Biểu đồ **Chi theo ngày** và **Cơ cấu theo danh mục**  
- **Giao dịch gần đây**

### 🧾 Giao dịch
- Form “Thêm nhanh” nhập nhanh các khoản thu/chi  
- Nhập tiền dạng `20.000.000`, chọn **Ngày** và **Giờ** giao dịch.

### 👛 Ví / Tài khoản
- Quản lý các ví: **Tiền mặt, Ngân hàng, Thẻ**  
- Hiển thị **Số dư hiện tại** (cập nhật theo giao dịch)

### 🏷️ Danh mục
- Tự tạo danh mục **Chi tiêu/Thu nhập**

### 🎯 Ngân sách
- Đặt **hạn mức** chi tiêu theo tháng, theo dõi % sử dụng

### 📈 Báo cáo
- **Top danh mục chi** + xuất **CSV** ở trang Cài đặt
    """)

def page_transactions(uid):
    st.subheader("🧾 Giao dịch")
    accounts = get_accounts(uid)
    cats_exp = get_categories(uid, "expense")
    cats_inc = get_categories(uid, "income")

    with st.expander("➕ Thêm giao dịch mới", expanded=True):
        ttype = st.radio("Loại giao dịch", ["Chi tiêu", "Thu nhập"], horizontal=True)
        acc = st.selectbox("Chọn ví/tài khoản", accounts["name"])
        cat = st.selectbox(
            "Chọn danh mục",
            cats_exp["name"] if ttype == "Chi tiêu" else cats_inc["name"]
        )

        # Nhập số tiền dạng text để hiện dấu chấm
        amt_text = st.text_input("Số tiền (VND)", placeholder="VD: 20.000.000")
        notes = st.text_input("Ghi chú (tùy chọn)")

        # Ngày + Giờ
        date = st.date_input("Ngày giao dịch", value=dt.date.today())
        time = st.time_input(
            "Giờ giao dịch",
            value=dt.datetime.now().time().replace(second=0, microsecond=0)
        )

        if st.button("Lưu giao dịch", use_container_width=True):
            try:
                amt = parse_vnd_str(amt_text)
                if amt <= 0:
                    st.error("Số tiền phải lớn hơn 0.")
                    st.stop()
                acc_id = int(accounts[accounts["name"] == acc]["id"].iloc[0])
                cats_df = (cats_exp if ttype == "Chi tiêu" else cats_inc)
                cat_id = int(cats_df[cats_df["name"] == cat]["id"].iloc[0])
                occurred_dt = join_date_time(date, time)
                add_transaction(uid, acc_id,
                                "expense" if ttype == "Chi tiêu" else "income",
                                cat_id, amt, notes, occurred_dt)
                st.success("✅ Giao dịch đã được lưu!")
            except Exception:
                st.error("Vui lòng nhập số tiền hợp lệ (ví dụ: 20.000.000).")

    st.divider()
    st.write("### 📊 Danh sách giao dịch gần đây")
    df = list_transactions(uid)
    df = df_tx_vi(df)
    if df is None or df.empty:
        st.info("Chưa có giao dịch nào.")
    else:
        st.dataframe(df, use_container_width=True, hide_index=True)

# =========================
# KPI & Charts cho Trang chủ
# =========================
def kpi_month(uid, start_date, end_date):
    row = fetchone("""
        SELECT
          COALESCE(SUM(CASE WHEN type='income'  THEN amount END),0) AS income,
          COALESCE(SUM(CASE WHEN type='expense' THEN amount END),0) AS expense
        FROM transactions
        WHERE user_id=? AND date(occurred_at) BETWEEN date(?) AND date(?)
    """, (uid, str(start_date), str(end_date)))
    income = float(row["income"] or 0)
    expense = float(row["expense"] or 0)
    net = income - expense
    c1, c2, c3 = st.columns(3)
    c1.metric("Tổng thu", f"{format_vnd(income)} VND")
    c2.metric("Tổng chi", f"{format_vnd(expense)} VND")
    c3.metric("Net (thu - chi)", f"{format_vnd(net)} VND")

def chart_spending_by_day(uid, start_date, end_date):
    df = get_df("""
        SELECT date(occurred_at) AS Ngày,
               SUM(CASE WHEN type='expense' THEN amount ELSE 0 END) AS Chi_tiêu
        FROM transactions
        WHERE user_id=? AND date(occurred_at) BETWEEN date(?) AND date(?)
        GROUP BY date(occurred_at)
        ORDER BY date(occurred_at)
    """, (uid, str(start_date), str(end_date)))
    if df.empty:
        st.info("Chưa có dữ liệu trong khoảng đã chọn.")
        return
    chart = alt.Chart(df).mark_line(point=True).encode(
        x=alt.X("Ngày:T", title="Ngày"),
        y=alt.Y("Chi_tiêu:Q", title="Chi tiêu (VND)"),
        tooltip=["Ngày", "Chi_tiêu"]
    ).properties(height=260)
    st.altair_chart(chart, use_container_width=True)

def chart_pie_by_category(uid, start_date, end_date):
    df = get_df("""
        SELECT COALESCE(c.name,'(Không danh mục)') AS Danh_mục,
               SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
        FROM transactions t
        LEFT JOIN categories c ON c.id=t.category_id
        WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
        GROUP BY c.name
        HAVING Chi_tiêu > 0
        ORDER BY Chi_tiêu DESC
    """, (uid, str(start_date), str(end_date)))
    if df.empty:
        st.info("Chưa có chi tiêu theo danh mục trong khoảng ngày.")
        return
    chart = alt.Chart(df).mark_arc().encode(
        theta=alt.Theta("Chi_tiêu:Q"),
        color=alt.Color("Danh_mục:N", legend=None),
        tooltip=["Danh_mục", "Chi_tiêu"]
    ).properties(height=260)
    st.altair_chart(chart, use_container_width=True)

# =========================
# Trang chủ
# =========================
def page_home(uid):
    st.subheader("🏠 Trang chủ")
    today = dt.date.today()
    if "filter_start" not in st.session_state:
        st.session_state.filter_start = today.replace(day=1)
    if "filter_end" not in st.session_state:
        st.session_state.filter_end = today

    c1, c2 = st.columns(2)
    st.session_state.filter_start = c1.date_input("Từ ngày", st.session_state.filter_start)
    st.session_state.filter_end = c2.date_input("Đến ngày", st.session_state.filter_end)

    st.divider()
    kpi_month(uid, st.session_state.filter_start, st.session_state.filter_end)

    colA, colB = st.columns([2, 1])
    with colA:
        st.markdown("#### Chi theo ngày")
        chart_spending_by_day(uid, st.session_state.filter_start, st.session_state.filter_end)
    with colB:
        st.markdown("#### Cơ cấu theo danh mục")
        chart_pie_by_category(uid, st.session_state.filter_start, st.session_state.filter_end)

    st.divider()
    st.markdown("#### Giao dịch gần đây")
    df = list_transactions(uid, today - dt.timedelta(days=7), today)
    df = df_tx_vi(df)
    if df.empty:
        st.info("Chưa có giao dịch nào tuần này. Nhấn **+ Thêm giao dịch** để ghi nhanh.")
    else:
        st.dataframe(df.head(10), use_container_width=True, height=260)
    if st.button("➕ Thêm giao dịch", type="primary"):
        st.session_state.nav = "Giao dịch"
        st.session_state.show_quick_add = True
        st.rerun()

# =========================
# Tính số dư hiện tại theo ví
# =========================
def current_balance(uid, account_id):
    row = fetchone("""
        SELECT
          (SELECT opening_balance FROM accounts WHERE id=? AND user_id=?) +
          COALESCE((SELECT SUM(amount) FROM transactions WHERE user_id=? AND account_id=? AND type='income'),0) -
          COALESCE((SELECT SUM(amount) FROM transactions WHERE user_id=? AND account_id=? AND type='expense'),0)
        AS bal
    """, (account_id, uid, uid, account_id, uid, account_id))
    return float(row["bal"] or 0.0)

def vi_account_display_names(df_accounts: pd.DataFrame):
    from collections import defaultdict
    counters = defaultdict(int); labels = []
    for _, r in df_accounts.iterrows():
        base = ACCOUNT_TYPE_LABEL_VN.get(r["type"], r["name"])
        counters[r["type"]] += 1
        suffix = "" if counters[r["type"]] == 1 else f" #{counters[r['type']]}"
        labels.append(f"{base}{suffix}")
    return labels

# =========================
# Trang Ví / Tài khoản
# =========================
def page_accounts(uid):
    st.subheader("👛 Ví / Tài khoản")
    df = get_accounts(uid)
    if df.empty:
        st.info("Chưa có ví nào. Hãy tạo ví đầu tiên của bạn 👇")
    else:
        df_disp = df.copy()
        df_disp["Hiển thị"] = vi_account_display_names(df_disp)
        df_disp["Loại"] = df_disp["type"].map(ACCOUNT_TYPE_LABEL_VN)
        balances = []
        for _, r in df_disp.iterrows():
            balances.append(current_balance(uid, int(r["id"])))
        df_disp["Số dư hiện tại"] = [format_vnd(x) for x in balances]
        df_disp = df_disp.rename(columns={
            "id": "ID", "currency": "Tiền tệ", "opening_balance": "Số dư ban đầu"
        })
        df_disp["Số dư ban đầu"] = df_disp["Số dư ban đầu"].map(lambda x: format_vnd(x))
        df_disp = df_disp[["ID","Hiển thị","Loại","Tiền tệ","Số dư ban đầu","Số dư hiện tại"]]
        st.dataframe(df_disp, use_container_width=True, height=320)

    st.markdown("#### Thêm ví mới")
    name = st.text_input("Tên ví (tuỳ chọn)")
    ttype = st.selectbox("Loại", ["cash","bank","card"], format_func=lambda x: ACCOUNT_TYPE_LABEL_VN.get(x, x))
    opening = st.number_input("Số dư ban đầu", min_value=0, step=1000)
    if st.button("Thêm ví", type="primary"):
        add_account(uid, name if name.strip() else ACCOUNT_TYPE_LABEL_VN.get(ttype, ttype), ttype, opening)
        st.success("Đã thêm ví!")
        st.rerun()

# =========================
# Trang Danh mục
# =========================
def page_categories(uid):
    st.subheader("🏷️ Danh mục")
    df = get_categories(uid)
    if df.empty:
        st.info("Chưa có danh mục nào. Tạo ít nhất 1 **Chi tiêu** và 1 **Thu nhập** để bắt đầu.")
    else:
        show = df.rename(columns={"id":"ID","name":"Tên","type":"Loại"})
        show["Loại"] = show["Loại"].map(TYPE_LABELS_VN)
        st.dataframe(show, use_container_width=True, height=300)

    st.markdown("#### Thêm danh mục")
    cname = st.text_input("Tên danh mục")
    ctype = st.selectbox("Loại", ["expense","income"], format_func=lambda x: TYPE_LABELS_VN[x])
    if st.button("Thêm danh mục", type="primary"):
        if cname.strip():
            add_category(uid, cname.strip(), ctype)
            st.success("Đã thêm danh mục!")
            st.rerun()
        else:
            st.error("Tên danh mục không được để trống.")

# =========================
# Ngân sách
# =========================
def page_budgets(uid):
    st.subheader("🎯 Ngân sách")
    st.caption("Đặt hạn mức chi tiêu theo khoảng ngày cho từng danh mục Chi tiêu.")

    cats_exp = get_categories(uid, "expense")
    if cats_exp.empty:
        st.info("Chưa có danh mục Chi tiêu. Vào **Danh mục** để tạo trước.")
        return

    cat = st.selectbox("Danh mục", cats_exp["name"])
    cat_id = int(cats_exp[cats_exp["name"] == cat]["id"].iloc[0])
    start = st.date_input("Từ ngày", value=dt.date.today().replace(day=1))
    end = st.date_input("Đến ngày", value=dt.date.today())
    amount = st.number_input("Hạn mức (VND)", min_value=0, step=100000)

    if st.button("Lưu hạn mức", type="primary"):
        execute("""INSERT INTO budgets(user_id,category_id,amount,start_date,end_date)
                   VALUES(?,?,?,?,?)""", (uid, cat_id, float(amount), str(start), str(end)))
        st.success("Đã lưu hạn mức!")

    st.divider()
    st.markdown("#### Hạn mức hiện có")
    df = get_df("""
        SELECT b.id, c.name AS category, b.amount, b.start_date, b.end_date
        FROM budgets b JOIN categories c ON c.id=b.category_id
        WHERE b.user_id=? ORDER BY b.start_date DESC
    """, (uid,))
    if df.empty:
        st.info("Chưa có hạn mức nào.")
    else:
        df = df.rename(columns={
            "id":"ID","category":"Danh mục","amount":"Hạn mức (VND)",
            "start_date":"Từ ngày","end_date":"Đến ngày"
        })
        df["Hạn mức (VND)"] = df["Hạn mức (VND)"].map(lambda x: format_vnd(x))
        st.dataframe(df, use_container_width=True, height=260)

# =========================
# Báo cáo
# =========================
def page_reports(uid):
    st.subheader("📈 Báo cáo")
    today = dt.date.today()
    start = st.date_input("Từ ngày", st.session_state.get("filter_start", today.replace(day=1)))
    end = st.date_input("Đến ngày", st.session_state.get("filter_end", today))
    st.session_state.filter_start = start; st.session_state.filter_end = end

    st.markdown("#### Top danh mục chi")
    df_cat = get_df("""
        SELECT COALESCE(c.name,'(Không danh mục)') AS Danh_mục,
               SUM(CASE WHEN t.type='expense' THEN t.amount ELSE 0 END) AS Chi_tiêu
        FROM transactions t LEFT JOIN categories c ON c.id=t.category_id
        WHERE t.user_id=? AND date(t.occurred_at) BETWEEN date(?) AND date(?)
        GROUP BY c.name HAVING Chi_tiêu>0 ORDER BY Chi_tiêu DESC LIMIT 10
    """,(uid, str(start), str(end)))
    if df_cat.empty:
        st.info("Chưa có dữ liệu.")
    else:
        st.altair_chart(
            alt.Chart(df_cat).mark_bar().encode(
                x=alt.X("Chi_tiêu:Q", title="Chi tiêu (VND)"),
                y=alt.Y("Danh_mục:N", sort='-x', title="Danh mục"),
                tooltip=["Danh_mục","Chi_tiêu"]
            ).properties(height=320),
            use_container_width=True
        )

    st.markdown("#### Danh sách giao dịch")
    df = list_transactions(uid, start, end)
    df = df_tx_vi(df)
    st.dataframe(df, use_container_width=True, height=360)

# =========================
# Cài đặt
# =========================
def page_settings(uid):
    st.subheader("⚙️ Cài đặt / Xuất dữ liệu")
    df = list_transactions(uid)
    if df.empty:
        st.info("Chưa có dữ liệu để tải.")
    else:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button("Tải transactions.csv", csv, file_name="transactions.csv", mime="text/csv")

# =========================
# Wizard thiết lập lần đầu (3 bước)
# =========================
def onboarding_wizard(uid):
    st.title("🚀 Thiết lập lần đầu")
    if "ob_step" not in st.session_state:
        st.session_state.ob_step = 1

    if st.session_state.ob_step == 1:
        name = st.text_input("Tên hiển thị của bạn", "")
        if st.button("Tiếp tục ➜", type="primary", disabled=(not name.strip())):
            set_user_profile(uid, name.strip()); st.session_state.ob_step = 2; st.rerun()

    elif st.session_state.ob_step == 2:
        st.write("Nhập số dư ban đầu cho ví (nhập kiểu **20.000.000**):")
        df = get_accounts(uid)
        try:
            cash_id = int(df[df["type"]=="cash"]["id"].iloc[0])
            bank_id = int(df[df["type"]=="bank"]["id"].iloc[0])
        except Exception:
            st.error("Không tìm thấy ví mặc định. Hãy đăng xuất và đăng ký lại.")
            return
        c1,c2 = st.columns(2)
        cash_text = c1.text_input("Tiền mặt (VND)", placeholder="VD: 2.000.000")
        bank_text = c2.text_input("Tài khoản ngân hàng (VND)", placeholder="VD: 8.000.000")

        if st.button("Lưu & tiếp tục ➜", type="primary"):
            cash = parse_vnd_str(cash_text)
            bank = parse_vnd_str(bank_text)
            execute("UPDATE accounts SET opening_balance=? WHERE id=?", (float(cash), cash_id))
            execute("UPDATE accounts SET opening_balance=? WHERE id=?", (float(bank), bank_id))
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
            st.dataframe(show, use_container_width=True, height=220)

        ok = (not get_categories(uid, "expense").empty) and (not get_categories(uid, "income").empty)
        if st.button("Hoàn tất", type="primary", disabled=(not ok)):
            finish_onboarding(uid); st.success("Xong! Bắt đầu dùng ứng dụng thôi 🎉"); st.rerun()

# =========================
# Màn hình Đăng nhập / Đăng ký
# =========================
def screen_login():
    st.title("💸 Expense Manager")
    st.caption("Quản lý chi tiêu cá nhân — Streamlit + SQLite")

    tab1, tab2 = st.tabs(["Đăng nhập", "Đăng ký"])

    with tab1:
        email = st.text_input("Email")
        pw = st.text_input("Mật khẩu", type="password")
        if st.button("Đăng nhập", type="primary", use_container_width=True):
            uid = login_user(email, pw)
            if uid:
                st.session_state.user_id = int(uid)
                st.success("Đăng nhập thành công!")
                st.rerun()
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
                if ok:
                    st.success(msg)
                    st.info("Bạn có thể đăng nhập ở tab bên cạnh.")
                else:
                    st.error(msg)

# =========================
# Sidebar + Router
# =========================
def app_shell(uid: int):
    u = get_user(uid)
    with st.sidebar:
        st.markdown("### 💶 Expense Manager")
        today = dt.date.today()
        st.write(f"👤 **{u['display_name'] or u['email']}**")
        st.caption(today.strftime("%d/%m/%Y"))

        menu_items = [
            "Trang chủ", "Giao dịch", "Ví/Tài khoản",
            "Danh mục", "Ngân sách", "Báo cáo",
            "Cài đặt", "Hướng dẫn sử dụng"
        ]
        nav = st.radio(
            "Điều hướng", menu_items,
            label_visibility="collapsed",
            index=menu_items.index(st.session_state.get("nav", "Trang chủ"))
        )
        st.session_state.nav = nav

        if st.button("Đăng xuất", use_container_width=True):
            st.session_state.clear()
            st.rerun()

    if nav == "Trang chủ":
        page_home(uid)
    elif nav == "Giao dịch":
        page_transactions(uid)
    elif nav == "Ví/Tài khoản":
        page_accounts(uid)
    elif nav == "Danh mục":
        page_categories(uid)
    elif nav == "Ngân sách":
        page_budgets(uid)
    elif nav == "Báo cáo":
        page_reports(uid)
    elif nav == "Cài đặt":
        page_settings(uid)
    else:
        page_help(uid)

# =========================
# Main entry
# =========================
def main():
    st.set_page_config(page_title="Expense Manager", page_icon="💸", layout="wide")
    init_db()

    if "user_id" not in st.session_state:
        screen_login()
        return

    # Nếu đã đăng nhập, kiểm tra đã hoàn tất onboarding chưa
    u = get_user(st.session_state.user_id)
    if not u:
        # user không tồn tại (có thể DB mới) -> xóa session và về login
        st.session_state.clear()
        screen_login()
        return

    if int(u["onboarded"] or 0) == 0:
        onboarding_wizard(st.session_state.user_id)
    else:
        app_shell(st.session_state.user_id)

if __name__ == "__main__":
    main()

