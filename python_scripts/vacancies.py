import pandas as pd
import re
from pathlib import Path
from . import helper, setup
from . import vacancies_errors

pd.options.mode.copy_on_write = True


#############################
# Load Vacancies from cache
#############################
def get_vacancies(basePath="./Original-CSVs", verbose=True):
    df, dffp = setup.setup(basepath=basePath, verbose=verbose)
    return df.vac


#################
# Load Vacancies
#################
def _get_vacancies_real(basePath="./Original-CSVs", verbose=True, do_cleaning=True):
    dir = Path(basePath)
    if verbose:
        print("Loading Vacancies.csv...")
    # Read in the CSV file
    df_vac = pd.read_csv(
        dir / "Vacancies.csv",
        dtype=df_vac_dtypes,
        parse_dates=df_vac_dates,
        dayfirst=True,
    )
    # Short column names
    df_vac = helper.short_cols(df_vac)
    # Short referral agency categories
    df_vac = df_vac.rename(columns={"ref_agency_1": "move_on_ref_agency"})
    df_vac["ref_agency_short"] = short_ref_agency(df_vac["ref_agency"])
    df_vac["ref_agency_short_padded"] = helper.col_padded(df_vac["ref_agency_short"])
    if do_cleaning:
        # Clean the data
        df_vac = correct_invalid_dates(df_vac)
        # Correct individual errors found through data exploration
        df_moved_vac, df_vac = vacancies_errors.correct_individual_errors(df_vac)
    else:
        df_moved_vac = df_vac.iloc[0:0]

    return df_moved_vac, df_vac


#########################
# Correct invalid dates
#########################
def correct_invalid_dates(df_vac: pd.DataFrame):
    # Correct invalid 'Notification Date' value: 'Vacancy Filled Date' is 30/01/2018
    df_vac.loc[df_vac["notification_dt"] == "07/02/1018", ["notification_dt"]] = (
        "07/02/2018"
    )
    df_vac["notification_dt"] = pd.to_datetime(df_vac["notification_dt"], dayfirst=True)
    # Correct invalid 'Vacancy Start_dt' value: 'Vacancy Filled Date' is 29/01/2016
    df_vac.loc[df_vac["vac_start_dt"] == "29/01/0201", ["vac_start_dt"]] = "29/01/2016"
    df_vac["vac_start_dt"] = pd.to_datetime(df_vac["vac_start_dt"], dayfirst=True)
    return df_vac


###########################
# Short referral agencies
###########################
def short_ref_agency(ref_agency: pd.Series):
    return ref_agency.copy().cat.rename_categories(
        lambda x: re.sub(r"(?<!Test)\.", "", x)
        .replace("1625 Independent People", "1625-IP")
        .replace("Bcc", "BCC")
        .replace(" - ", " ")
        .replace("Children & Families", "Ch&Fam")
        .replace("Homelessness Prevention Team", "HPT")
        .replace("Landlord", "Lndlrd")
        .replace("Prevention Team", "Prev")
        .replace("Private Rented Team", "PRT")
        .replace("Specialist Advisors Team", "SpAd")
        .replace("Supported Lettings (Rapid Rehousing Pathway)", "Sup Lets")
        .replace("Tenant Support Service", "TSS")
        .replace("Voids Team", "Voids")
        .replace("South Glos", "S Glos")
        .replace("ECHG Riverside", "Riverside")
        .replace("Missing Link", "Miss Lnk")
        .replace("Places For People", "PFP")
        .replace("Priority Youth", "PYH")
        .replace("Prison Resettlement", "Prison Rstlm")
        .replace("Salvation Army", "Salv Army")
        .replace("Sanctuary Carr-Gomm", "Sanct CG")
        .replace("St Mungo's", "Mungos")
        .replace("Through Care", "Throughcare")
        .replace("BCC Streetwise", "Streetwise")
        .replace("St James Priory", "St J Priory")
        # .replace(r"B\.C\.C\.?( -)?", "BCC", regex=True)
        # .str.replace(" *accommodation based ?-? ?", "", case=False, regex=True)
        # .str.replace("Young People", "YP", case=False)
        # .str.replace("Specialist Adult Services - Non-Pathway", "Non-Pathway Specialist Adult")
    )


#########################
# Data types for fields
#########################
df_vac_dtypes = {
    "Pseudo Vacancy ID": "Int64",
    "Vacancy address outward code": "category",
    "Vacancy End Date": "str",
    "Pseudo referral ID": "Int64",
    "Filled By Referral Interview Date": "str",
    "Filled By Referral Nomination Date": "str",
    "Referral Agency": "category",
    "Filled By Referral Date": "str",
    "Referral Type": "category",
    "Filled By Referral Result Date": "str",
    "Referral Result Set on": "str",
    "Vacancy Filled Date": "str",
    "Filled Date Entered By": "string",
    "Filled Date Entered On": "str",
    "Filled Type": "category",
    "Move-On Date": "str",
    "Client involved in Training": "category",
    "Client involved in Education": "category",
    "Client involved in Employment": "category",
    "Moved Out Date": "str",
    "Notification Date": "str",
    "Overnight Placement": "category",
    "Placement End Reason": "category",
    "Placement Reference": "Int64",
    "Placement Owner": "category",
    "Placement Support Category": "Int64",
    "Referral Agency.1": "category",
    "Service Contract Id": "Int64",
    "Shared": "category",
    "Vacancy Start Date": "str",
    "Start Date Entered By": "string",
    "Start Date Entered On": "str",
    "TA Duty": "Int64",
    "TA Bedrooms": "Int64",
    "TA Other LA": "Int64",
    "TA Type": "Int64",
    "Vacancy Close Date Set On": "str",
    "Vacancy Days": "Int64",
    "Vacancy End Time": "Int64",
    "Filled By Referral - Result Date": "str",
    "Vacancy Filled Time": "Int64",
    "Placement Duration Days": "Int64",
    "Placement Duration Nights": "Int64",
    "Pseudo service ID": "Int64",
    "Pseudo client ID": "Int64",
}


##########################################
# Fields which should be parsed as dates
##########################################
df_vac_dates = [
    "Vacancy End Date",
    "Filled By Referral Interview Date",
    "Filled By Referral Nomination Date",
    "Filled By Referral Date",
    "Filled By Referral Result Date",
    "Referral Result Set on",
    "Vacancy Filled Date",
    "Filled Date Entered On",
    "Move-On Date",
    "Moved Out Date",
    "Notification Date",
    "Vacancy Start Date",
    "Start Date Entered On",
    "Vacancy Close Date Set On",
    "Filled By Referral - Result Date",
]
