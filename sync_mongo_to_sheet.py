from pymongo import MongoClient
import gspread
import os 
from google.oauth2.service_account import Credentials
from datetime import datetime
from dateutil.relativedelta import relativedelta
import time
from gspread.exceptions import APIError

from gspread_formatting import (
    set_data_validation_for_cell_range,
    DataValidationRule,
    BooleanCondition,
    CellFormat,
    Color,
    ConditionalFormatRule,
    BooleanRule,
    get_conditional_format_rules,
    GridRange,
    format_cell_range,
    set_row_height,         # ✅ ADDED
    set_column_width        # ✅ ADDED
)

# ================== CONFIG ==================

MONGO_URI = os.environ.get("MONGO_URI")
DB_NAME = "Grower"
COLLECTION_NAME = "Member"

SERVICE_ACCOUNT_FILE = "credentials.json"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1jx-WM6g948mzjWxNa1qY8Osmf0qx8XXh9hugBuC7G9M/edit"
WORKSHEET_NAME = "Sheet1"

DATA_START_ROW = 8
ID_COL = 1  # Column A

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

# ================== AUTH ==================

creds = Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=SCOPES
)

gc = gspread.authorize(creds)
ws = gc.open_by_url(SPREADSHEET_URL).worksheet(WORKSHEET_NAME)

# ================== HELPERS ==================

def fmt_date(value):
    if isinstance(value, datetime):
        return value.strftime("%d-%b-%Y")
    return ""

def calculate_valid_till(last_payment, plan):
    if not isinstance(last_payment, datetime):
        return ""

    plan = str(plan).strip().lower()

    if plan == "monthly":
        return last_payment + relativedelta(months=1)
    if plan == "3month":
        return last_payment + relativedelta(months=3)
    if plan == "yearly":
        return last_payment + relativedelta(years=1)

    return last_payment

def col_letter(n):
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# REQUIRED for IMAGE()
def safe_update(range_name, values, retries=5):
    for i in range(retries):
        try:
            ws.update(
                range_name=range_name,
                values=values,
                value_input_option="USER_ENTERED"
            )
            return
        except APIError:
            time.sleep(2 ** i)

# ================== READ EXISTING IDS ==================

existing_ids = ws.col_values(ID_COL)[DATA_START_ROW - 1:]
id_to_row = {}

for i, v in enumerate(existing_ids):
    if v.strip():
        id_to_row[v.strip()] = DATA_START_ROW + i

next_row = DATA_START_ROW + len(existing_ids)

# ================== MONGODB UPSERT ==================

mongo = MongoClient(MONGO_URI)
collection = mongo[DB_NAME][COLLECTION_NAME]

for doc in collection.find():
    mongo_id = str(doc["_id"])
    plan = doc.get("plan", "")
    last_payment = doc.get("lastPayment")
    valid_till = calculate_valid_till(last_payment, plan)

    photo_url = doc.get("photo")
    photo_formula = f'=IMAGE("{photo_url}", 4, 120, 120)' if photo_url else ""

    row_data = [
        mongo_id,
        doc.get("name", ""),
        doc.get("street", ""),
        fmt_date(doc.get("dateOfBirth")),
        doc.get("bloodGroup", ""),
        str(doc.get("whatsappNumber", "")),
        plan,
        photo_formula,
        doc.get("fees", 0),
        "Paid" if doc.get("isPaid") else "Not Paid",
        fmt_date(doc.get("createdAt")),
        fmt_date(last_payment),
        fmt_date(valid_till),
        fmt_date(doc.get("updatedAt")),
    ]

    row = id_to_row.get(mongo_id, next_row)
    if mongo_id not in id_to_row:
        id_to_row[mongo_id] = row
        next_row += 1

    end_col = col_letter(len(row_data))
    safe_update(f"A{row}:{end_col}{row}", [row_data])

# ================== DROPDOWNS ==================

set_data_validation_for_cell_range(
    ws, "G8:G1000",
    DataValidationRule(BooleanCondition("ONE_OF_LIST", ["monthly", "3month", "yearly"]), showCustomUi=True)
)

set_data_validation_for_cell_range(
    ws, "J8:J1000",
    DataValidationRule(BooleanCondition("ONE_OF_LIST", ["Paid", "Not Paid"]), showCustomUi=True)
)

# ================== CONDITIONAL FORMATTING ==================

rules = get_conditional_format_rules(ws)
rules.clear()

# Plan colors
rules.append(ConditionalFormatRule(
    ranges=[GridRange.from_a1_range("G8:G1000", ws)],
    booleanRule=BooleanRule(BooleanCondition("TEXT_EQ", ["monthly"]),
    CellFormat(backgroundColor=Color(0.8, 0.9, 1)))
))

rules.append(ConditionalFormatRule(
    ranges=[GridRange.from_a1_range("G8:G1000", ws)],
    booleanRule=BooleanRule(BooleanCondition("TEXT_EQ", ["3month"]),
    CellFormat(backgroundColor=Color(0.85, 1, 0.85)))
))

rules.append(ConditionalFormatRule(
    ranges=[GridRange.from_a1_range("G8:G1000", ws)],
    booleanRule=BooleanRule(BooleanCondition("TEXT_EQ", ["yearly"]),
    CellFormat(backgroundColor=Color(0.9, 0.85, 1)))
))

# Paid cell green
rules.append(ConditionalFormatRule(
    ranges=[GridRange.from_a1_range("J8:J1000", ws)],
    booleanRule=BooleanRule(BooleanCondition("TEXT_EQ", ["Paid"]),
    CellFormat(backgroundColor=Color(0.75, 0.95, 0.75)))
))

# Full row red if Not Paid
rules.append(ConditionalFormatRule(
    ranges=[GridRange.from_a1_range("A8:N1000", ws)],
    booleanRule=BooleanRule(BooleanCondition("CUSTOM_FORMULA", ['=$J8="Not Paid"']),
    CellFormat(backgroundColor=Color(1, 0.9, 0.9)))
))

rules.save()

# ================== ROW HEIGHT + PHOTO COLUMN WIDTH (✅ ONLY NEW ADDITION) ==================

# Photo column = H
set_column_width(ws, "H", 130)

# Row height for images
set_row_height(ws, f"{DATA_START_ROW}:1000", 120)

# ================== DATA TYPE FORMATTING ==================

format_cell_range(ws, "A8:A1000", CellFormat(numberFormat={"type": "TEXT"}))
format_cell_range(ws, "B8:C1000", CellFormat(numberFormat={"type": "TEXT"}))
format_cell_range(ws, "D8:D1000", CellFormat(numberFormat={"type": "DATE"}))
format_cell_range(ws, "E8:E1000", CellFormat(numberFormat={"type": "TEXT"}))
format_cell_range(ws, "F8:F1000", CellFormat(numberFormat={"type": "TEXT"}))
format_cell_range(ws, "I8:I1000", CellFormat(numberFormat={"type": "CURRENCY", "pattern": "₹#,##0"}))
format_cell_range(ws, "K8:N1000", CellFormat(numberFormat={"type": "DATE"}))

# ================== ALIGNMENT ==================

format_cell_range(ws, "A8:N1000", CellFormat(
    horizontalAlignment="CENTER",
    verticalAlignment="MIDDLE"
))

print("✅ SUCCESS: MongoDB → Google Sheets sync (DATA TYPES + PROFESSIONAL FORMATTING) completed")