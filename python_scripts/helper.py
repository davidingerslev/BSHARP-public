import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


########################
# Shorten column names
########################
def short_cols(df: pd.DataFrame, symbol_replacement=""):
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(".", "_")
        .str.replace("pseudo_", "")
        .str.replace("vacancy_", "vac_")
        .str.replace("client_", "cli_")
        .str.replace("service_", "svc_")
        .str.replace("referral_", "ref_")
        .str.replace("trusted_assessment_", "tr_a_")
        .str.replace("_date", "_dt")
        .str.replace("_group", "_grp")
        .str.replace("orig_", "o_")
        .str.replace("how_does_applicant_define_their_gender", "gender")
        .str.replace("[^A-Za-z0-9_]", symbol_replacement, regex=True)
        .str.replace("_$", "", regex=True)
    )
    return df


#########################
# Pad column categories
#########################
def col_padded(col: pd.Series):
    return col.str.pad(width=max(col.dtype.categories.str.len()), side="right").astype(
        "category"
    )


#########################################################
# Remove unused categories from all categorical columns
#########################################################
def remove_unused_categories(df: pd.DataFrame):
    for c in df.columns:
        if isinstance(df[c].dtype, pd.CategoricalDtype):
            df[c] = df[c].cat.remove_unused_categories()
    return df


#######################################
# Return value counts and percentages
#######################################
def value_counts_and_pcts(data, dropna=True, pct_digits=1, sort_index=False):
    FT = pd.concat(
        {
            "n": data.value_counts(dropna=dropna),
            "%": data.value_counts(dropna=dropna, normalize=True).mul(100),
        },
        axis=1,
    )
    if sort_index:
        FT = FT.sort_index()
    FT.loc["Total"] = FT.sum(numeric_only=True)
    kwargs = {"%": FT["%"].apply(lambda p: f"{p:.{pct_digits}f}%")}
    FT = FT.astype({"n": "Int64"}).assign(**kwargs)
    FT = FT.set_index(FT.index.fillna(pd.NA)).rename_axis(None)
    return FT


def bar_chart(tbl: pd.DataFrame, cat: str):
    tbl_body = (
        tbl.set_index(tbl.index.fillna("<NA>"))
        .drop("Total")
        .reset_index(names=cat)
        .rename(columns={"n": "Count"})
    )
    ax = sns.barplot(tbl_body, x=cat, y="Count")
    plt.rcParams["font.size"] = 8
    ax.bar_label(ax.containers[0], labels=tbl_body["%"])
    return ax


def age_cats(ages):
    age_cats = {
        0: "<18",
        18: "18-24",
        25: "25-34",
        35: "35-44",
        45: "45-54",
        55: "55-64",
        65: ">=65",
    }
    return pd.cut(
        ages,
        bins=list(age_cats.keys()) + [999],
        labels=list(v for _, v in age_cats.items()),
        right=False,
    )


def age_cats_bgp(ages):  # Age categories used in Blood, Goldup and Pleace (2023)
    age_cats = {
        0: "<18",
        18: "18-25",
        26: "26-39",
        40: "40-64",
        65: ">=65",
    }
    return pd.cut(
        ages,
        bins=list(age_cats.keys()) + [999],
        labels=list(v for _, v in age_cats.items()),
        right=False,
    )


#############################################
# Filter to a snapshot of a particular date
#############################################
def get_snapshot(df_placements: pd.DataFrame, start_date, end_date=None, filter=None):
    dffp = df_placements
    if end_date is None:
        end_date = start_date
    date_filter = (dffp.pl_start_dt <= end_date) & (
        (dffp.pl_end_dt >= start_date)  # Ended since DPW started
        | dffp.pl_end_dt.isna()  # Still open
    )
    if filter is None:
        filter = date_filter
    else:
        filter = filter & date_filter
    return dffp[filter]


#################################################################
# Logging function: flexible approach in case of future changes
#################################################################
def log(*args, verbose=True, **kwargs):
    if verbose:
        print(*args, **kwargs)
