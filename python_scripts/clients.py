import pandas as pd
from . import helper, setup
from pathlib import Path

pd.options.mode.copy_on_write = True


###########################
# Load Clients From Cache
###########################
def get_clients(basePath="./Original-CSVs", verbose=True):
    df, dffp = setup.setup(basepath=basePath, verbose=verbose)
    return df.cli


################
# Load Clients
################
def _get_clients_real(basePath="./Original-CSVs", verbose=True, do_cleaning=True):
    dir = Path(basePath)
    if verbose:
        print("Loading Clients.csv...")
    df_cli = pd.read_csv(
        dir / "Clients.csv",
        dtype=df_cli_dtypes,
        parse_dates=df_cli_dates,
        dayfirst=True,
    )
    # Short column names
    df_cli = helper.short_cols(df_cli)
    # Order the categories with specified orders
    for col in df_cli.columns[df_cli.columns.isin(category_order)]:
        df_cli[col] = df_cli[col].cat.set_categories(category_order[col], ordered=True)
    # Add o_cli_id to all records, not just those with duplicates identified
    df_cli["o_cli_id"] = df_cli["min_cli_id_for_possible_duplicates"].fillna(
        df_cli["cli_id"]
    )
    # Remove test clients
    df_cli = df_cli[df_cli["test_clients"].isna()]
    # Clean the data
    if do_cleaning:
        df_cli = clean_clients(df_cli)
    return df_cli


#################
# Data cleaning
#################
def clean_clients(df_cli: pd.DataFrame):
    # Fix error in Registration Date
    df_cli.loc[df_cli["registration_dt"] == "05/11/1013", ["registration_dt"]] = (
        "05/11/2013"
    )
    df_cli["registration_dt"] = pd.to_datetime(
        df_cli["registration_dt"], dayfirst=True, errors="coerce"
    )

    # Fix multiple errors in Housing Status Date
    df_cli["housing_status_dt"] = df_cli["housing_status_dt"].replace(
        {
            # Repetitive, so likely entering a seemingly valid date to allow form entry:
            "01/01/1011": pd.NaT,
            "12/12/1210": pd.NaT,
            # Unclear, but not relevant as the person has no placements:
            "06/09/1010": pd.NaT,
            # Likely typo, given context:
            "20/12/1093": "20/12/2023",  # Reg and next assessment date 2023
            "15/11/0018": "15/11/2018",  # Reg and next assessment date 2018
        }
    )
    df_cli["housing_status_dt"] = pd.to_datetime(
        df_cli["housing_status_dt"], dayfirst=True, errors="coerce"
    )

    ######################################################
    # Fix individual errors found through data exploration
    ######################################################
    # cli_id 321 should not have o_cli_id 1496, it should be 321.
    df_cli.loc[(df_cli.cli_id == 321), "o_cli_id"] = 321
    # cli_id 10445 and 16817 should not have the same o_cli_id. Error in identifying duplicate NINO.
    df_cli.loc[(df_cli.cli_id == 10445), "o_cli_id"] = 10445
    # cli_id 28862 and 932 should not have been linked together -- possibly typo in NINO.
    df_cli.loc[(df_cli.cli_id == 932), "o_cli_id"] = 932
    # cli_id 21769 and 25304 may be two people with the same NINO entered
    # Checked original database: this is two separate people with the same NINO recorded.
    df_cli.loc[(df_cli.cli_id == 25304), "o_cli_id"] = 25304

    ##########################################################################
    # Fix individual errors found through characteristic exploration: gender
    ##########################################################################
    # cli_ids 5890, 29148 are different people.
    for CID in [5890, 29148]:
        df_cli.loc[df_cli.cli_id == CID, "o_cli_id"] = CID
    # Assume "Don't Know" for gender for cli_id == 252
    df_cli.loc[df_cli.cli_id == 252, "gender"] = "Don't Know"

    return df_cli


#########################
# Data types for fields
#########################
df_cli_dtypes = {
    "Pseudo client ID": "Int64",
    "Test clients": "category",
    "Pseudo Min client ID for possible duplicates": "Int64",
    "Household code": "string",
    "YOB": "Int64",
    "QOB": "Int64",
    "Marital Status": "category",
    "Registration Date": "str",
    "Agency Name": "category",
    "Primary Needs": "category",
    "Secondary Needs": "category",
    "Housing Status": "category",
    "Housing Status Date": "str",
    "Everyone In": "float64",
    "Risk Type": "category",
    "Caseowner": "string",
    "Caseowner Team": "category",
    "Next Assessment Date": "str",
    "Closed Reason": "category",
    "Closed Date": "str",
    "Ethnicity": "category",
    "Religion/Beliefs": "category",
    "Sexual Orientation": "category",
    "Nationality": "category",
    "Nationality Category": "category",
    "Gender Description": "float64",
    "Gender Identity": "float64",
    "Gender Is Birth Gender?": "float64",
    "When did Applicant arrive in Bristol?": "category",
    "Transgender": "category",
    "English 2nd Language": "category",
    "Languages Spoken": "string",
    "How does Applicant define their gender?": "category",
    "Immigration Status": "float64",
    "Registered With GP": "category",
    "Registered Disabled": "category",
    "Disability": "category",
    "Does client consider themselves disabled": "category",
    "Benefit Status": "category",
    "Benefit Status Date": "str",
    "Covered By DDA": "category",
    "Local Connection": "float64",
    "Communication - British Sign L": "category",
    "Communication difficulty with ": "category",
    "Communication difficulty with .1": "category",
    "Disability - Health related lo": "category",
    "Disability - Hearing impairmen": "category",
    "Disability - Learning impairme": "category",
    "Disability - Mental/emotional ": "category",
    "Disability - Mobility impairme": "category",
    "Disability - None": "category",
    "Disability - Prefer not to say": "category",
    "Disability - Visual impairment": "category",
    "Disability - Wheelchair user": "category",
    "Part 7 - Section 20 status?": "float64",
    "Primary Disability": "float64",
}


##########################################
# Fields which should be parsed as dates
##########################################
df_cli_dates = [
    # 'Registration Date',  # Exclude due to errors
    # 'Housing Status Date', # Exclude due to errors
    "Closed Date",
    "Benefit Status Date",
    "Next Assessment Date",
]


###############################################
# Order for categoricals with specific orders
###############################################
category_order = {
    "when_did_applicant_arrive_in_bristol": [
        "Within the last week",
        "Between a week and a month ago",
        "Between 1 and 6 months ago",
        "Over 6 months ago/lived here all life",
        "Not Bristol Resident (Add notes in Events tab)",
    ]
}
