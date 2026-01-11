import pandas as pd
import json
from datetime import datetime


# ---------------------------------------------------------
# REQUIRED FIELDS
# ---------------------------------------------------------
REQUIRED_FIELDS = [
    "providerCcn",
    "effectiveDate",
    "stateCode",
    "providerType",
    "fiscalYearBeginDate",
    "fiscalYearEndDate",
    "exportDate",
    "lastUpdated"
]

# ---------------------------------------------------------
# DATE FIELDS
# ---------------------------------------------------------
DATE_FIELDS = [
    "effectiveDate",
    "fiscalYearBeginDate",
    "fiscalYearEndDate",
    "exportDate",
    "terminationDate",
    "lastUpdated"
]

# ---------------------------------------------------------
# NUMERIC FIELDS
# ---------------------------------------------------------
NUMERIC_FIELDS = [
    "operatingCostToChargeRatio",
    "capitalCostToChargeRatio",
    "specialProviderUpdateFactor",
    "supplementalSecurityIncomeRatio",
    "medicaidRatio",
    "uncompensatedCareAmount"
]


# ---------------------------------------------------------
# HELPER FUNCTIONS
# ---------------------------------------------------------
def is_missing(val):
    return val is None or pd.isna(val) or (isinstance(val, str) and val.strip() == "")


def is_valid_date(val):
    if is_missing(val):
        return True
    try:
        pd.to_datetime(val, errors="raise")
        return True
    except Exception:
        return False


def issue(row, idx, issue_type, details):
    return {
        "provider_id": row.get("providerCcn") or row.get("nationalProviderIdentifier"),
        "issue_type": issue_type,
        "issue_details": details,
        "row_data": json.dumps(row.to_dict()),
        "detected_at": datetime.utcnow(),
        "row_index": idx
    }


# ---------------------------------------------------------
# COMPLETENESS CHECKS
# ---------------------------------------------------------
def check_completeness(df):
    issues = []
    for idx, row in df.iterrows():
        for col in REQUIRED_FIELDS:
            if is_missing(row.get(col)):
                issues.append(issue(row, idx, "Missing Value", f"{col} is required"))
    return issues


# ---------------------------------------------------------
# VALIDITY CHECKS (ALL NEW RULES INCLUDED)
# ---------------------------------------------------------
def check_validity(df):
    issues = []
    for idx, row in df.iterrows():

        # -------------------------------------------------
        # providerCcn: 6–13 digit numeric
        # -------------------------------------------------
        cc = row.get("providerCcn")
        if not is_missing(cc):
            cc_str = str(cc).strip()
            if not (cc_str.isdigit() and 6 <= len(cc_str) <= 13):
                issues.append(issue(row, idx, "Invalid CCN",
                                    "providerCcn must be 6–13 digit numeric"))

        # -------------------------------------------------
        # stateCode: 2-digit numeric
        # -------------------------------------------------
        sc = row.get("stateCode")
        if not is_missing(sc):
            sc_str = str(sc).strip()
            if not (sc_str.isdigit() and len(sc_str) == 2):
                issues.append(issue(row, idx, "Invalid State Code",
                                    "stateCode must be a 2-digit numeric value"))

        # -------------------------------------------------
        # NEW: waiverIndicator must be Y or N
        # -------------------------------------------------
        wi = row.get("waiverIndicator")
        if not is_missing(wi):
            wi_str = str(wi).strip()
            if wi_str not in ["Y", "N"]:
                issues.append(issue(row, idx, "Invalid Waiver Indicator",
                                    "waiverIndicator must be one of: Y, N"))

        # -------------------------------------------------
        # NEW: intermediaryNumber must be 5-digit numeric
        # -------------------------------------------------
        im = row.get("intermediaryNumber")
        if not is_missing(im):
            im_str = str(im).strip()
            if not (im_str.isdigit() and len(im_str) == 5):
                issues.append(issue(row, idx, "Invalid Intermediary Number",
                                    "intermediaryNumber must be 5-digit numeric"))

        # -------------------------------------------------
        # NEW: providerType must be 2-digit numeric
        # -------------------------------------------------
        pt = row.get("providerType")
        if not is_missing(pt):
            pt_str = str(pt).strip()
            if not (pt_str.isdigit() and len(pt_str) == 2):
                issues.append(issue(row, idx, "Invalid Provider Type",
                                    "providerType must be 2-digit numeric"))

        # -------------------------------------------------
        # NEW: msaActualGeographicLocation must be 2 or 4 digit numeric
        # -------------------------------------------------
        msa = row.get("msaActualGeographicLocation")
        if not is_missing(msa):
            msa_str = str(msa).strip()
            if not (msa_str.isdigit() and len(msa_str) in [2, 4]):
                issues.append(issue(row, idx, "Invalid MSA Geographic Location",
                                    "msaActualGeographicLocation must be 2 or 4 digit numeric"))

        # -------------------------------------------------
        # NEW: nationalProviderIdentifier must be 10-digit numeric
        # -------------------------------------------------
        npi = row.get("nationalProviderIdentifier")
        if not is_missing(npi):
            npi_str = str(npi).strip()
            if not (npi_str.isdigit() and len(npi_str) == 10):
                issues.append(issue(row, idx, "Invalid NPI",
                                    "nationalProviderIdentifier must be 10-digit numeric"))

        # -------------------------------------------------
        # DATE FIELDS
        # -------------------------------------------------
        for col in DATE_FIELDS:
            if not is_valid_date(row.get(col)):
                issues.append(issue(row, idx, "Invalid Date",
                                    f"{col} has invalid date format"))

        # -------------------------------------------------
        # NUMERIC FIELDS
        # -------------------------------------------------
        for col in NUMERIC_FIELDS:
            val = row.get(col)
            if is_missing(val):
                continue
            try:
                float(val)
            except:
                issues.append(issue(row, idx, "Invalid Numeric",
                                    f"{col} must be numeric"))

    return issues


# ---------------------------------------------------------
# UNIQUENESS CHECKS
# ---------------------------------------------------------
def check_uniqueness(df):
    issues = []
    dup = df[df.duplicated(
        ["providerCcn", "effectiveDate", "nationalProviderIdentifier"],
        keep=False
    )]
    for idx, row in dup.iterrows():
        issues.append(issue(row, idx, "Duplicate Row",
                            "Duplicate providerCcn + effectiveDate + NPI"))
    return issues


# ---------------------------------------------------------
# RUN ALL CHECKS
# ---------------------------------------------------------
def run_all_dq_checks(df):
    issues = []
    issues.extend(check_completeness(df))
    issues.extend(check_validity(df))
    issues.extend(check_uniqueness(df))
    return issues