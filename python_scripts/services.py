import pandas as pd
from . import helper, setup
from pathlib import Path

pd.options.mode.copy_on_write = True


############################
# Load Services from cache
############################
def get_services(basePath="./Original-CSVs", verbose=True):
    df, dffp = setup.setup(basepath=basePath, verbose=verbose)
    return df.svc


################
# Load Services
################
def _get_services_real(basePath="./Original-CSVs", verbose=True, do_cleaning=True):
    dir = Path(basePath)
    if verbose:
        print("Loading Services.csv...")
    df_svc = pd.read_csv(dir / "Services.csv", dtype=df_svc_dtypes)
    # Short column names
    df_svc = helper.short_cols(df_svc)
    df_svc = df_svc.rename(columns={"svc_capacity_categorised": "svc_capacity"})
    if do_cleaning:
        # Clean the data
        df_svc = clean_services(df_svc)
    # Remap service types and pathway levels for OABs
    df_svc = remap_OABs(df_svc, basePath, verbose)
    # Short service type categories
    df_svc["svc_type_short"] = short_svc_types(df_svc["svc_type"])
    df_svc["svc_type_short_padded"] = helper.col_padded(df_svc["svc_type_short"])
    # Add data about levels under pre-distinct Pathways and post-distinct Pathways models
    df_svc = add_apwlvl(df_svc)
    return df_svc


#######################################################################
# Add data about levels under pre- and post- distinct-pathways models
#######################################################################
def add_apwlvl(df_svc: pd.DataFrame):
    # Manual adjustments: { (svc_type, pathway_level): awp_lvl }
    apwlvl_dict_manual = {
        ("D&A - Abstinent", pd.NA): "DA_Abst",
        ("Emergency L1", pd.NA): "Pre_1",
        ("High Support L2", pd.NA): "Pre_2",
        ("Medium Support L3", pd.NA): "Pre_3",
        ("Substance Misuse Pathway", pd.NA): "SMU_NA",
        # TODO: Check this in confidential data: SMU_NA likely an error
    }
    apwlvl_df_manual = pd.DataFrame(
        apwlvl_dict_manual.values(),
        index=apwlvl_dict_manual.keys(),
        columns=["apwlvl"],
    )
    # Calculated adjustments: DPW_ except substance misuse pathway which has
    # a different meaning for different levels.
    svc_types_with_levels = (
        df_svc[["svc_type_short", "pathway_level"]].drop_duplicates().dropna()
    )
    svc_types_with_levels["svc_apwlvl"] = (
        "DPW_" + svc_types_with_levels.pathway_level.astype("string")
    ).where(
        svc_types_with_levels.svc_type_short != "Substance Misuse Pathway",
        other=("SMU_" + svc_types_with_levels.pathway_level.astype("string")),
    )
    apwlvl_map = svc_types_with_levels.set_index(["svc_type_short", "pathway_level"])
    apwlvl_map = pd.concat([apwlvl_map, apwlvl_df_manual]).sort_index()

    df_svc = df_svc.set_index(["svc_type_short", "pathway_level"]).sort_index()
    df_svc["svc_apwlvl"] = pd.NA
    df_svc.update(apwlvl_map)
    df_svc.reset_index(inplace=True)
    return df_svc


################################################
# Remap Service Types (OABs and Pathway Levels)
################################################
def remap_OABs(df_svc: pd.DataFrame, basePath="./Original-CSVs", verbose=True):
    dir = Path(basePath)
    filename = "OAB_service_type_updates.csv"
    helper.log(f"Loading {filename}...", verbose=verbose)
    df_oabmaps = pd.read_csv(dir / filename, dtype=df_oabmaps_dtypes)
    df_oabmaps = helper.short_cols(df_oabmaps)

    # Remap service types for services using replcements from the updates data
    df_oabmaps = df_oabmaps.set_index("svc_id")
    df_svc.set_index("svc_id", inplace=True)
    df_svc.svc_type = df_svc.svc_type.astype("string")
    df_svc.update(df_oabmaps.updated_svc_type.rename("svc_type"))
    df_svc.reset_index(inplace=True)
    df_oabmaps.reset_index(inplace=True)

    # Create a new variable identifying OAB services
    df_svc["OAB"] = df_svc.svc_id.isin(df_oabmaps.svc_id)

    # TODO: Check confidential data and reset Pseudo-IDs for OAB services, they're
    # really part of the host service.

    # Remap pathway levels: OAB is about particular ways of accessing, not different levels
    df_svc.pathway_level = df_svc.pathway_level.replace(
        {"1 OAB": "1", "2 OAB": "2"}
    ).astype("category")

    return df_svc


#################
# Data cleaning
#################
def clean_services(df_svc: pd.DataFrame):
    # Correct capacity ranges that Excel interpreted as dates(!)
    capacity_corrections = {"01-Mar": "1-3", "04-Jun": "4-6", "Jul-14": "7-14"}
    df_svc.svc_capacity = df_svc.svc_capacity.replace(capacity_corrections)
    ordered_capcats = df_svc.svc_capacity.cat.categories.sort_values(
        key=lambda x: x.str.split("[-+]", regex=True).str.get(0).astype("int")
    )
    df_svc.svc_capacity = df_svc.svc_capacity.cat.reorder_categories(
        ordered_capcats, ordered=True
    )
    return df_svc


#######################
# Short service types
#######################
def short_svc_types(svc_type: pd.Series):
    return (
        svc_type.str.replace(" *accommodation based ?-? ?", "", case=False, regex=True)
        .str.replace("Young People", "YP", case=False)
        .str.replace(
            "Specialist Adult Services - Non-Pathway", "Non-Pathway Specialist Adult"
        )
        .str.replace(" - ISAT use only", "")
        .str.replace("Assessment - HSR - Administration Only", "HSR Assessment (ADMIN)")
        .str.replace(
            "Non-pathway accommodation (temporary housing in empty buildings)",
            "Non-pathway THEB",
        )
        .str.replace("Floating Support - ", "FS: ")
        .str.replace("Rough Sleepers Initiative (RSI)", "RSI accom")
        .str.replace("-Level One$", " L1", case=False, regex=True)
        .str.replace("-Level Two$", " L2", case=False, regex=True)
        .str.replace("-Level Three$", " L3", case=False, regex=True)
        .str.replace(" and ", " / ", case=False)
        .astype("category")
    )


#########################
# Data types for fields
#########################
df_svc_dtypes = {
    "Pseudo service ID": "Int64",
    "Service Type": "category",
    "Pathway level": "category",
    "Service capacity categorised": "category",
    "Clients Accepted": "category",
    "Minimum Age": "Int64",
    "Maximum Age": "Int64",
    "Primary Client Group": "category",
    "Secondary Client Group": "category",
    "Service Support Level": "category",
    "Pseudo Provider Id": "Int64",
}
df_oabmaps_dtypes = {
    "Pseudo service ID": "Int64",
    "Service Type": "string",
    "Updated Service Type": "string",
    "Pathway level": "string",
}

####################################################################
# Which service types are accommodation and Pathways accommodation
####################################################################
accom_svc_types = [
    # "Accommodation Based - External Support Accom (ESA)",  # This includes floating support
    "Accommodation Based - Family",
    "Accommodation Based - Female Only Pathway",
    "Accommodation Based - Long Stay",
    "Accommodation Based - Male Only Pathway",
    "Accommodation Based - Mixed Pathway",
    "Accommodation Based - Outreach Access Beds (OAB)",
    "Accommodation Based - Parent & Baby and Specialist Young People",
    "Accommodation Based - Singles and couple non pathway",
    "Accommodation Based - Specialist Adult Services - Non-Pathway",  # Includes RSPS, HF, HSH, RR
    "Accommodation Based - Substance Misuse Pathway",
    "Accommodation Based - Supported Move-on",
    "Accommodation Based - Young People Pathway",
    "Accommodation Based-Emergency-Level One",
    "Accommodation Based-High Support-Level Two",
    "Accommodation Based-Medium Support-Level Three",
    "Accommodation based - D&A - Abstinent",
    # 'Assessment - HSR - Administration Only',
    # 'Enhanced Access services - ISAT use only',
    # 'Floating Support',
    # 'Floating Support - Drug and Alcohol Service',
    # 'Floating Support - Family Hostels',
    # 'Floating Support - MH Complex',
    # 'Floating Support - MH Crisis',
    # 'Floating Support - MH Standard',
    # 'Floating Support - Short Term - Resettlement',
    "Rough Sleepers Initiative (RSI) Services",
    "SSTS   Accommodation Based",
    # 'SSTS - Floating Support',
    "Non-pathway accommodation (temporary housing in empty buildings)",
]
adult_pathways_accom_svc_types = [
    # "Accommodation Based - External Support Accom (ESA)",  # This includes floating support
    # 'Accommodation Based - Family',
    "Accommodation Based - Female Only Pathway",
    # "Accommodation Based - Long Stay",
    "Accommodation Based - Male Only Pathway",
    "Accommodation Based - Mixed Pathway",
    "Accommodation Based - Outreach Access Beds (OAB)",
    # 'Accommodation Based - Parent & Baby and Specialist Young People',
    # 'Accommodation Based - Singles and couple non pathway',
    # 'Accommodation Based - Specialist Adult Services - Non-Pathway',
    "Accommodation Based - Substance Misuse Pathway",
    "Accommodation Based - Supported Move-on",
    # 'Accommodation Based - Young People Pathway',
    "Accommodation Based-Emergency-Level One",
    "Accommodation Based-High Support-Level Two",
    "Accommodation Based-Medium Support-Level Three",
    "Accommodation based - D&A - Abstinent",
    # 'Assessment - HSR - Administration Only',
    # 'Enhanced Access services - ISAT use only',
    # 'Floating Support',
    # 'Floating Support - Drug and Alcohol Service',
    # 'Floating Support - Family Hostels',
    # 'Floating Support - MH Complex',
    # 'Floating Support - MH Crisis',
    # 'Floating Support - MH Standard',
    # 'Floating Support - Short Term - Resettlement',
    "Rough Sleepers Initiative (RSI) Services",
    # 'SSTS   Accommodation Based',
    # 'SSTS - Floating Support',
    # 'Non-pathway accommodation (temporary housing in empty buildings)'
]
distinct_pathways_accom_svc_types = [
    # "Accommodation Based - External Support Accom (ESA)",  # This includes floating support
    # 'Accommodation Based - Family',
    "Accommodation Based - Female Only Pathway",
    # "Accommodation Based - Long Stay",
    "Accommodation Based - Male Only Pathway",
    "Accommodation Based - Mixed Pathway",
    "Accommodation Based - Outreach Access Beds (OAB)",
    # 'Accommodation Based - Parent & Baby and Specialist Young People',
    # 'Accommodation Based - Singles and couple non pathway',
    # 'Accommodation Based - Specialist Adult Services - Non-Pathway',
    "Accommodation Based - Substance Misuse Pathway",
    # "Accommodation Based - Supported Move-on",
    # 'Accommodation Based - Young People Pathway',
    # "Accommodation Based-Emergency-Level One",
    # "Accommodation Based-High Support-Level Two",
    # "Accommodation Based-Medium Support-Level Three",
    # "Accommodation based - D&A - Abstinent",
    # 'Assessment - HSR - Administration Only',
    # 'Enhanced Access services - ISAT use only',
    # 'Floating Support',
    # 'Floating Support - Drug and Alcohol Service',
    # 'Floating Support - Family Hostels',
    # 'Floating Support - MH Complex',
    # 'Floating Support - MH Crisis',
    # 'Floating Support - MH Standard',
    # 'Floating Support - Short Term - Resettlement',
    # "Rough Sleepers Initiative (RSI) Services",
    # 'SSTS   Accommodation Based',
    # 'SSTS - Floating Support',
    # 'Non-pathway accommodation (temporary housing in empty buildings)'
]
