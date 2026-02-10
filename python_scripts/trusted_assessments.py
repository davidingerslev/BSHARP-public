import pandas as pd
from . import helper, setup
from pathlib import Path

pd.options.mode.copy_on_write = True


#######################################
# Load Trusted Assessments From Cache
#######################################
def get_trusted_assessments(basePath="./Original-CSVs", verbose=True):
    df, dffp = setup.setup(basepath=basePath, verbose=verbose)
    return df.tr_a


############################
# Load Trusted Assessments
############################
def _get_trusted_assessments_real(
    basePath="./Original-CSVs", verbose=True, do_cleaning=True
):
    dir = Path(basePath)
    if verbose:
        print("Loading Trusted Assessments.csv...")
    df_tr_a = pd.read_csv(
        dir / "Trusted Assessments.csv",
        dtype=tr_a_dtypes,
        parse_dates=tr_a_dates,
        dayfirst=True,
    )
    # Short column names
    df_tr_a = df_tr_a.rename(columns=tr_a_col_map)
    if do_cleaning:
        # Clean the data
        df_tr_a = clean_trusted_assessments(df_tr_a)
    # Set the index
    df_tr_a = df_tr_a.set_index("tr_a_id")
    # Load support needs
    for filename, prefix in [
        ("Trusted Assessments - Support Needs.csv", "hsn"),
        ("Trusted Assessments - Floating Support Needs.csv", "fsn"),
    ]:
        if verbose:
            print(f"Loading {filename}...")
        df_tr_a_sn = pd.read_csv(
            dir / filename,
            dtype=tr_a_sn_dtypes[prefix],
            usecols=tr_a_sn_dtypes[prefix].keys(),
        )
        df_tr_a_sn = helper.short_cols(df_tr_a_sn, symbol_replacement="_")
        df_tr_a_sn = df_tr_a_sn.dropna(subset="tr_a_id").set_index("tr_a_id")
        df_tr_a = df_tr_a.join(
            helper.short_cols(pd.get_dummies(df_tr_a_sn, prefix=prefix))
            .groupby(level=0)
            .any(),
            on="tr_a_id",
            how="left",
        )
    return df_tr_a


def clean_trusted_assessments(df_tr_a: pd.DataFrame):
    df_tr_a.loc[df_tr_a["ntq_expiry_dt"] == "13/12/2021", ["ntq_expiry_dt"]] = (
        "13/12/2021"
    )
    df_tr_a["ntq_expiry_dt"] = pd.to_datetime(
        df_tr_a["ntq_expiry_dt"], dayfirst=True, errors="coerce"
    )
    df_tr_a.loc[
        df_tr_a["new_tenancy_start_dt"] == "01/01/1000", ["new_tenancy_start_dt"]
    ] = pd.NaT
    df_tr_a["new_tenancy_start_dt"] = pd.to_datetime(
        df_tr_a["new_tenancy_start_dt"], dayfirst=True, errors="coerce"
    )
    return df_tr_a


tr_a_sn_dtypes = {
    "hsn": {
        "Pseudo Trusted Assessment Id": "Int64",
        "Support need": "category",
    },
    "fsn": {
        "Pseudo Trusted Assessment Id": "Int64",
        "Floating support need": "category",
    },
}

tr_a_dtypes = {
    "Pseudo Trusted Assessment Id": "Int64",
    "Sensitivity Level": "category",
    "Pseudo Client ID": "Int64",
    "004 Gender Identity": "string",
    "YOB": "Int64",
    "QOB": "Int64",
    "012 Do you need an interpreter?": "category",
    "013 Main language": "string",
    "014 Other languages": "string",
    "015 Do you need a sign language interpreter?": "string",
    "016 Is this referral for a single applicant or a couple/family?": "category",
    "018 Are you (or any other member of your household) pregnant?": "category",
    "020 What time have you spent living in Bristol? Prompt: This is what is called Local Connection. Please ensure you have included evidence of this in the Addresses tab": "category",
    "021 Do you have a Local Connection via a close family member (mother, father, brother or sister) who has been resident in Bristol for the last five years?": "category",
    "023 Are you currently working in Bristol in ongoing employment?": "category",
    "025 Do you have unique special circumstances for Local Connection? Prompt: Tick No if they are being referred before Local Connection has been established under the Homelessness legislation": "category",
    "027 Current status in the UK": "category",
    "y_llr_expires": "Int64",
    "q_llr_expires": "Int64",
    "030 h_total_needs": "Int64",
    "033 Are you, the referrer, referring to accommodation on the HSR (either for singles or families and including D&A services)?": "category",
    "035 Is the client currently placed in HSR supported accommodation? (If the client is being immediately evicted from HSR accommodation, please still answer Yes). PLEASE NOTE that emergency accommodatio": "category",
    "040 For referrer use   What level of accommodation support do you feel this client requires?": "category",
    "041 Has the client been given a notice to quit?": "category",
    "044 Is the client currently rough sleeping?": "category",
    "049 Are you, the main applicant, currently working?": "category",
    "051 Is any other member of your household currently working?": "category",
    "053 Total of earnings for the household per week (GBP). If not employed, please enter 0": "string",
    "056 Total amount received from benefits per week (GBP)": "string",
    "058 Amount (GBP) per week from other income sources": "string",
    "059 Total household income (GBP) per week": "string",
    "062 Does the client have any outstanding debts or arrears from previous tenancies? Prompt: It is important that this question is answered correctly. The client should be reassured that having rent arr": "category",
    "064 Is there a repayment plan in place?": "category",
    "065 Does the client have a live HomeChoice Bristol application?": "category",
    "067 What Band are they in?": "category",
    "068 Do you (or anyone else in your household) have any pets?": "category",
    "069 Accommodation that will accept pets is unfortunately quite rare. Are you willing to be accommodated without your pet(s) in order to access supported accommodation?": "category",
    "075 Do you have any criminal convictions or open investigations following an arrest?": "category",
    "077 Is this a current or a past risk?": "category",
    "082 Is there any history of setting fire to things?": "category",
    "084 Is this a current or a past risk?": "category",
    "088 Have there been any incidents where you have been violent towards someone or something? NB: Please include details of any hateful and discriminatory abuse, plus any support needs in relation to th": "category",
    "090 Is this a current or a past risk?": "category",
    "094 Is there a history of sexual assaults, abuse or sexually inappropriate behaviour from you?": "category",
    "096 Is this a current or a past risk?": "category",
    "100 Do you use drugs/alcohol/other substances? Prompt: Please still state Yes if the client is currently in recovery.": "category",
    "102 Is this current or in the past?": "category",
    "108 Trusted Assessment: Do you smoke tobacco?": "category",
    "110 Do you currently have any concerns about your mental health/wellbeing? Are you trying to improve your mental health at the moment?": "category",
    "112 Is this a current or a past issue?": "category",
    "116 Trusted Assessment: Do you currently have any concerns about your physical health? Are you trying to improve your physical health at the moment?": "category",
    "117 Is this a current or a past issue?": "category",
    "118 Trusted Assessment: Do you consider yourself to have a disability?": "category",
    "124 Trusted Assessment: Have you ever intentionally hurt yourself or tried to take your own life? Prompt: Framing this question with the client is really important.": "category",
    "125 Is this a current or a past risk?": "category",
    "129 Trusted Assessment: Do you consider yourself to have a learning difficulty or learning disability? Prompt: This should also include being unable to read or write": "category",
    "142 Do you, the referrer, want to add a second Risk Assessment for a member of the household aged 16 or over?": "category",
    "144 Do you have any criminal convictions or open investigations following an arrest?": "category",
    "146 Is this a current or a past risk?": "category",
    "151 Is there any history of setting fire to things?": "category",
    "153 Is this a current or a past risk?": "category",
    "157 Have there been any incidents where you have been violent towards someone or something? NB: Please include details of any hateful and discriminatory abuse, plus any support needs in relation to th": "category",
    "159 Is this a current or a past risk?": "category",
    "163 Is there a history of sexual assaults, abuse or sexually inappropriate behaviour from you?": "category",
    "165 Is this a current or a past risk?": "category",
    "169 Do you use drugs/alcohol/other substances? Prompt: Please still state Yes if the client is currently in recovery.": "category",
    "171 Is this current or in the past?": "category",
    "177 Trusted Assessment: Do you smoke tobacco?": "category",
    "179 Do you currently have any concerns about your mental health/wellbeing? Are you trying to improve your mental health at the moment?": "category",
    "181 Is this a current or a past issue?": "category",
    "185 Trusted Assessment: Do you currently have any concerns about your physical health? Are you trying to improve your physical health at the moment?": "category",
    "186 Is this a current or a past issue?": "category",
    "187 Trusted Assessment: Do you consider yourself to have a disability?": "category",
    "193 Trusted Assessment: Have you ever intentionally hurt yourself or tried to take your own life? Prompt: Framing this question with the client is really important.": "category",
    "194 Is this a current or a past risk?": "category",
    "198 Trusted Assessment: Do you consider yourself to have a learning difficulty or learning disability? Prompt: This should also include being unable to read or write": "category",
    "211 Do you, the referrer, want to add a third Risk Assessment for a member of the household aged 16 or over?": "category",
    "280 Do you, the referrer, want to add a fourth Risk Assessment for a member of the household aged 16 or over?": "category",
    "349 Do you, the referrer, want to add a fifth Risk Assessment for a member of the household aged 16 or over?": "category",
    "418 Do you, the referrer, want to add a sixth Risk Assessment for a member of the household aged 16 or over?": "category",
    "487 Do you, the referrer, want to add a seventh Risk Assessment for a member of the household aged 16 or over?": "category",
    "556 Do you, the referrer, want to add an eighth Risk Assessment for a member of the household aged 16 or over?": "category",
    "625 Do you, the referrer, want to add a ninth Risk Assessment for a member of the household aged 16 or over?": "category",
    "694 Do you, the referrer, want to add a tenth Risk Assessment for a member of the household aged 16 or over?": "category",
    "763 Do you, the referrer, want to add an eleventh Risk Assessment for a member of the household aged 16 or over?": "category",
    "832 Are you, the referrer, referring a client for Floating Support?": "category",
    "833 Does the client need support to move to and sustain a new tenancy?": "category",
    "836 Does the client need support to sustain a current tenancy?": "category",
    "837 Tenancy type": "category",
    "838 Is the client at risk of losing their current home?": "category",
    "841 Based on your answer above, which type of floating support service are you referring to?": "category",
    "845 fs_total_needs": "Int64",
    "851 Are you, the referrer, referring from one HSR supported accommodation provider to another HSR accommodation provider?": "category",
    "852 Are you, the referrer, referring to accommodation with a higher, lower or the same level of support as your service?": "category",
    "854 Is the client under notice or being evicted from your service? If yes, please make sure the Eviction Protocol is followed (see Policy and Guidance page).": "category",
    "Form Name": "category",
    "Form Status": "category",
}

tr_a_dates = [
    "003 Date form completed or updated",
    "042 Date notice expires",
    "835 New tenancy start date",
    "855 Date notice expires",
    "879 Date form signed off",
    "Creation Date",
    "Form Status Date",
    "Last Updated Date",
]

tr_a_col_map = {
    "Pseudo Trusted Assessment Id": "tr_a_id",
    "Sensitivity Level": "sensitivity_level",
    "Pseudo Client ID": "cli_id",
    "003 Date form completed or updated": "completed_dt",
    "004 Gender Identity": "tr_a_gender",
    "YOB": "tr_a_yob",
    "QOB": "tr_a_qob",
    "012 Do you need an interpreter?": "interpreter_needed",
    "013 Main language": "main_language",
    "014 Other languages": "other_languages",
    "015 Do you need a sign language interpreter?": "sign_language_interpreter",
    "016 Is this referral for a single applicant or a couple/family?": "single_or_couple",
    "018 Are you (or any other member of your household) pregnant?": "pregnancy",
    "020 What time have you spent living in Bristol? Prompt: This is what is called Local Connection. Please ensure you have included evidence of this in the Addresses tab": "time_in_bristol",
    "021 Do you have a Local Connection via a close family member (mother, father, brother or sister) who has been resident in Bristol for the last five years?": "family_local_connection",
    "023 Are you currently working in Bristol in ongoing employment?": "employment_in_bristol",
    "025 Do you have unique special circumstances for Local Connection? Prompt: Tick No if they are being referred before Local Connection has been established under the Homelessness legislation": "special_local_connection",
    "027 Current status in the UK": "uk_immigration_status",
    "y_llr_expires": "y_llr_expires",
    "q_llr_expires": "q_llr_expires",
    "030 h_total_needs": "housing_support_needs_count",
    "033 Are you, the referrer, referring to accommodation on the HSR (either for singles or families and including D&A services)?": "referring_supported_accom",
    "035 Is the client currently placed in HSR supported accommodation? (If the client is being immediately evicted from HSR accommodation, please still answer Yes). PLEASE NOTE that emergency accommodatio": "in_hsr_accom",
    "040 For referrer use   What level of accommodation support do you feel this client requires?": "referring_support_level",
    "041 Has the client been given a notice to quit?": "has_ntq",
    "042 Date notice expires": "ntq_expiry_dt",
    "044 Is the client currently rough sleeping?": "is_rough_sleeping",
    "049 Are you, the main applicant, currently working?": "applicant_currently_working",
    "051 Is any other member of your household currently working?": "other_currently_working",
    "053 Total of earnings for the household per week (GBP). If not employed, please enter 0": "weekly_earned_income",
    "056 Total amount received from benefits per week (GBP)": "weekly_benefit_income",
    "058 Amount (GBP) per week from other income sources": "other_weekly_income",
    "059 Total household income (GBP) per week": "total_weekly_income",
    "062 Does the client have any outstanding debts or arrears from previous tenancies? Prompt: It is important that this question is answered correctly. The client should be reassured that having rent arr": "has_debts",
    "064 Is there a repayment plan in place?": "has_debt_repayment_plan",
    "065 Does the client have a live HomeChoice Bristol application?": "has_homechoice_application",
    "067 What Band are they in?": "homechoice_band",
    "068 Do you (or anyone else in your household) have any pets?": "has_pets",
    "069 Accommodation that will accept pets is unfortunately quite rare. Are you willing to be accommodated without your pet(s) in order to access supported accommodation?": "willing_leave_pets",
    "075 Do you have any criminal convictions or open investigations following an arrest?": "risk_crime_yn",
    "077 Is this a current or a past risk?": "risk_crime_current",
    "082 Is there any history of setting fire to things?": "risk_fire_yn",
    "084 Is this a current or a past risk?": "risk_fire_current",
    "088 Have there been any incidents where you have been violent towards someone or something? NB: Please include details of any hateful and discriminatory abuse, plus any support needs in relation to th": "risk_violence_hatecrime_yn",
    "090 Is this a current or a past risk?": "risk_violence_hatecrime_current",
    "094 Is there a history of sexual assaults, abuse or sexually inappropriate behaviour from you?": "risk_sexual_yn",
    "096 Is this a current or a past risk?": "risk_sexual_current",
    "100 Do you use drugs/alcohol/other substances? Prompt: Please still state Yes if the client is currently in recovery.": "risk_drugs_alcohol_yn",
    "102 Is this current or in the past?": "risk_drugs_alcohol_current",
    "108 Trusted Assessment: Do you smoke tobacco?": "smoke_tobacco",
    "110 Do you currently have any concerns about your mental health/wellbeing? Are you trying to improve your mental health at the moment?": "risk_mental_health_yn",
    "112 Is this a current or a past issue?": "risk_mental_health_current",
    "116 Trusted Assessment: Do you currently have any concerns about your physical health? Are you trying to improve your physical health at the moment?": "risk_physical_health_yn",
    "117 Is this a current or a past issue?": "risk_physical_health_current",
    "118 Trusted Assessment: Do you consider yourself to have a disability?": "considers_self_disabled",
    "124 Trusted Assessment: Have you ever intentionally hurt yourself or tried to take your own life? Prompt: Framing this question with the client is really important.": "risk_suicide_selfharm_yn",
    "125 Is this a current or a past risk?": "risk_suicide_selfharm_current",
    "129 Trusted Assessment: Do you consider yourself to have a learning difficulty or learning disability? Prompt: This should also include being unable to read or write": "has_learning_difficulty",
    "142 Do you, the referrer, want to add a second Risk Assessment for a member of the household aged 16 or over?": "person02_risk_assessment",
    "144 Do you have any criminal convictions or open investigations following an arrest?": "person02_risk_crime_yn",
    "146 Is this a current or a past risk?": "person02_risk_crime_current",
    "151 Is there any history of setting fire to things?": "person02_risk_fire_yn",
    "153 Is this a current or a past risk?": "person02_risk_fire_current",
    "157 Have there been any incidents where you have been violent towards someone or something? NB: Please include details of any hateful and discriminatory abuse, plus any support needs in relation to th": "person02_risk_violence_hatecrime_yn",
    "159 Is this a current or a past risk?": "person02_risk_violence_hatecrime_current",
    "163 Is there a history of sexual assaults, abuse or sexually inappropriate behaviour from you?": "person02_risk_sexual_yn",
    "165 Is this a current or a past risk?": "person02_risk_sexual_current",
    "169 Do you use drugs/alcohol/other substances? Prompt: Please still state Yes if the client is currently in recovery.": "person02_risk_drugs_alcohol_yn",
    "171 Is this current or in the past?": "person02_risk_drugs_alcohol_current",
    "177 Trusted Assessment: Do you smoke tobacco?": "person02_smoke_tobacco",
    "179 Do you currently have any concerns about your mental health/wellbeing? Are you trying to improve your mental health at the moment?": "person02_risk_mental_health_yn",
    "181 Is this a current or a past issue?": "person02_risk_mental_health_current",
    "185 Trusted Assessment: Do you currently have any concerns about your physical health? Are you trying to improve your physical health at the moment?": "person02_risk_physical_health_yn",
    "186 Is this a current or a past issue?": "person02_risk_physical_health_current",
    "187 Trusted Assessment: Do you consider yourself to have a disability?": "person02_considers_self_disabled",
    "193 Trusted Assessment: Have you ever intentionally hurt yourself or tried to take your own life? Prompt: Framing this question with the client is really important.": "person02_risk_suicide_selfharm_yn",
    "194 Is this a current or a past risk?": "person02_risk_suicide_selfharm_current",
    "198 Trusted Assessment: Do you consider yourself to have a learning difficulty or learning disability? Prompt: This should also include being unable to read or write": "person02_has_learning_difficulty",
    "211 Do you, the referrer, want to add a third Risk Assessment for a member of the household aged 16 or over?": "person03_risk_assessment",
    "280 Do you, the referrer, want to add a fourth Risk Assessment for a member of the household aged 16 or over?": "person04_risk_assessment",
    "349 Do you, the referrer, want to add a fifth Risk Assessment for a member of the household aged 16 or over?": "person05_risk_assessment",
    "418 Do you, the referrer, want to add a sixth Risk Assessment for a member of the household aged 16 or over?": "person06_risk_assessment",
    "487 Do you, the referrer, want to add a seventh Risk Assessment for a member of the household aged 16 or over?": "person07_risk_assessment",
    "556 Do you, the referrer, want to add an eighth Risk Assessment for a member of the household aged 16 or over?": "person08_risk_assessment",
    "625 Do you, the referrer, want to add a ninth Risk Assessment for a member of the household aged 16 or over?": "person09_risk_assessment",
    "694 Do you, the referrer, want to add a tenth Risk Assessment for a member of the household aged 16 or over?": "person10_risk_assessment",
    "763 Do you, the referrer, want to add an eleventh Risk Assessment for a member of the household aged 16 or over?": "person11_risk_assessment",
    "832 Are you, the referrer, referring a client for Floating Support?": "referring_floating_support",
    "833 Does the client need support to move to and sustain a new tenancy?": "need_support_new_tenancy",
    "835 New tenancy start date": "new_tenancy_start_dt",
    "836 Does the client need support to sustain a current tenancy?": "need_support_current_tenancy",
    "837 Tenancy type": "current_tenancy_type",
    "838 Is the client at risk of losing their current home?": "current_tenancy_at_risk",
    "841 Based on your answer above, which type of floating support service are you referring to?": "referring_floating_support_type",
    "845 fs_total_needs": "floating_support_needs_count",
    "851 Are you, the referrer, referring from one HSR supported accommodation provider to another HSR accommodation provider?": "referring_hsr_accom_to_hsr_accom",
    "852 Are you, the referrer, referring to accommodation with a higher, lower or the same level of support as your service?": "referring_hsr_comparative_level",
    "854 Is the client under notice or being evicted from your service? If yes, please make sure the Eviction Protocol is followed (see Policy and Guidance page).": "referring_being_evicted",
    "855 Date notice expires": "eviction_notice_expiry_dt",
    "879 Date form signed off": "tr_a_signed_dt",
    "Creation Date": "tr_a_created_dt",
    "Form Name": "form_name",
    "Form Status": "form_status",
    "Form Status Date": "form_status_dt",
    "Last Updated Date": "last_updated_dt",
}
