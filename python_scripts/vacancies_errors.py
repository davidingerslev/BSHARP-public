import pandas as pd
from enum import StrEnum


class CorrectionType(StrEnum):
    start_dt = "start_dt"
    end_dt = "end_dt"
    end_reason = "end_reason"
    vac_id_to_move = "move-vac"
    vac_id_to_delete = "delete-vac"
    svc_id = "svc_id"


class Corrections:
    def __init__(self):
        self._corrections = {}
        for x in CorrectionType:
            self._corrections[x] = []

    def add(
        self,
        type: CorrectionType,
        vac_id: int,
        correction: str | None = None,
        assumption: bool = False,
    ):
        self._corrections[type].append([vac_id, correction, assumption])

    def correct(self, df_vac: pd.DataFrame, include_assumptions: bool = True):
        # Copy because we're updating values via loc[] which would otherwise overwrite
        # the original
        df_copy = df_vac.copy()

        # Start dates
        for vac_id, correction, is_an_assumption in self._corrections["start_dt"]:
            if include_assumptions or not is_an_assumption:
                df_copy.loc[(df_copy.vac_id == vac_id), "vac_filled_dt"] = pd.Timestamp(
                    correction
                )

        # End dates
        for vac_id, correction, is_an_assumption in self._corrections["end_dt"]:
            if include_assumptions or not is_an_assumption:
                df_copy.loc[(df_copy.vac_id == vac_id), "moved_out_dt"] = pd.Timestamp(
                    correction
                )

        # End reasons
        for vac_id, correction, is_an_assumption in self._corrections["end_reason"]:
            if include_assumptions or not is_an_assumption:
                df_copy.loc[(df_copy.vac_id == vac_id), "placement_end_reason"] = (
                    correction
                )

        # Service IDs
        for vac_id, correction, is_an_assumption in self._corrections["svc_id"]:
            if include_assumptions or not is_an_assumption:
                df_copy.loc[(df_copy.vac_id == vac_id), "svc_id"] = int(correction)

        # Deleting vacancies
        vac_ids_to_delete = []
        for vac_id, _, is_an_assumption in self._corrections["delete-vac"]:
            if include_assumptions or not is_an_assumption:
                vac_ids_to_delete.append(vac_id)
        df_copy = df_copy[(~df_copy.vac_id.isin(vac_ids_to_delete))]

        # Moving vacancies to a new dataframe
        vac_ids_to_move = []
        for vac_id, _, is_an_assumption in self._corrections["move-vac"]:
            if include_assumptions or not is_an_assumption:
                vac_ids_to_move.append(vac_id)
        df_moved = df_copy[df_copy.vac_id.isin(vac_ids_to_move)]
        df_copy = df_copy[~df_copy.vac_id.isin(vac_ids_to_move)]

        return df_moved, df_copy


def correct_individual_errors(
    df_vac: pd.DataFrame,
    df_moved_vac: pd.DataFrame | None = None,
    include_assumptions: bool = True,
):
    corrections = get_manual_corrections(df_vac)

    df_new, df_vac = corrections.correct(df_vac, include_assumptions)

    if df_moved_vac is None:
        df_moved_vac = df_new
    else:
        df_moved_vac = pd.concat([df_moved_vac, df_new])

    return df_moved_vac, df_vac


def get_manual_corrections(df_vac: pd.DataFrame):
    corrections = Corrections()
    #############################################################
    # Errors found from wrong order in the Placement Start Date
    #############################################################

    # cli_id 1638 -- likely wrong end date (year) for vac_id==12916 which probably should be 2009-11-09 instead of
    # 2010-11-09. Checked original database and assumption is reasonable.
    corrections.add("end_dt", 12916, "2009-11-09")

    # cli_id 2664 -- there are continuous placements from 2012-2015, but 2 short placements overlapping with
    # the L4 placement. The short stays (vac_id.isin([8539, 54273])) perhaps relate to times when the person could not
    # stay in their L4 property.
    for x in [8539, 54273]:
        corrections.add("move-vac", x)

    # cli_id 4488 likely had the wrong placement ended -- if end date and reason were transposed between
    # vac_id.isin([3752,32256]) then this could make sense.
    corrections.add("end_dt", 3752, "2013-06-26")
    corrections.add("end_dt", 32256, "2013-02-02")

    # cli_id 7379 had an emergency placement for one week before going back into the same service
    corrections.add("move-vac", 12509)

    # cli_id 7387 appears to have a placement that was not closed down (vac_id==55420). It's possible the wrong vacancy
    # was closed, as that vacancy started on the date the previous one ended. (Category: old placement not closed down)
    # Checked original database: placement should have been closed down before 21/6/2011
    corrections.add("end_dt", 55420, "2011-06-20", assumption=True)

    # cli_id 7702 had vac_id==26848 backdated to the wrong date, should have been 2012-06-07.
    corrections.add("start_dt", 26848, "2012-06-07")

    # cli_id 8307 has two "Accommodation Based - External Support Accom (ESA)" placements, one of which is still open,
    # that started before an "Accommodation Based - Specialist Adult Services - Non-Pathway" placement had ended.
    # Perhaps don't include either service type in the analysis?
    # Checked original database -- this is not an accommodation serivce (necessarily)

    # cli_id 8490 has a placement where the referral agency was Test1. But other than that it seems logical -- there is
    # a 7-day Level 1 placement towards the end of a 344-day Level Two placement, it is likely the person was not able
    # to stay in the first placement for this period. vac_id == 36399
    corrections.add("move-vac", 36399)

    # cli_id 8784 has an old Level One placement that was not closed down from 2009, which should probably be discounted.
    # (Category: old placement not closed down)
    # Checked original database: placement should have been closed down before 05/06/2009
    corrections.add("end_dt", 17049, "2009-06-04", assumption=True)

    # cli_id 8998 has a placement where the referral agency was Test2 and also two concurrent placements (11 days and 20
    # days) in the substance use pathway -- perhaps this was an internal move and the backdating was done incorrectly,
    # or perhaps the end date was incorrect (or both) because the end reason was a planned move into supported housing
    # and then there was a gap. (Category: unclear)
    # Checked original database and established that the end reason for the gap was valid, a move to
    # non-commissioned "supported" housing.
    corrections.add("start_dt", 16527, "2012-01-16", assumption=True)
    corrections.add("end_dt", 16527, "2012-06-15", assumption=True)

    # cli_id 11529 had a 7-day stay in Level 1 of the male pathway during a placement in the YP pathway
    corrections.add("move-vac", 49892)

    # cli_id 12573 has an old Level One placement that was not closed down from 2009, which should probably be discounted.
    # Checked original database: placement should have been closed down on 12/05/2009
    corrections.add("end_dt", 6075, "2009-05-12")

    # cli_id 12923 has placements in both male-only and female-only pathways. But the wrong-order issue seems to be
    # caused by two placements in 'Accommodation Based - External Support Accom (ESA)' which started during a long
    # placement in the female-only pathway. They also have a 10-day placement in the mixed pathway which is during a
    # placement in the female-only pathway, which probably should be discounted. ESA placements are a 0-day placement in
    # service 165 and then a placement in service 44 that hasn't ended. Need to look at what that means. (2 issues)
    # Checked original database and the stay in male only accommodation is valid (low support, likely self-contained)
    # and ESA service was not accomodation.
    corrections.add("move-vac", 12835, assumption=True)

    # cli_id 13374 appears to have had the wrong placement closed down, if the placement_end_reasons and moved_out_dts were
    # swapped between vac_id.isin([54968,27659]) then it looks like it would all make sense.
    corrections.add("end_dt", 54968, "2013-06-26")
    corrections.add("end_dt", 27659, "2013-04-15")

    # cli_id 14310 has the same ESA placements issue as cli_ids 12923 and 8307
    # Checked original database: service type is not always accommodation.

    # cli_id 14445 has a placement that was never closed down in service 47 (Level 3) from 2011 and a later stay in
    # family accommodation. Either two different people whose records have been conflated, or vac_id 9418 should have
    # been closed earlier.
    # Checked original database and 2011 placement should have been closed down but unclear when:
    # sometime between 14/11/2011 and 21/10/2014. Best to exclude this placement as it was a standalone L3 placement,
    # not linked to any others and unclear how long.
    corrections.add("delete-vac", 9418, assumption=True)

    # cli_id 15517 appears to have had vac_id 49017 backdated to the wrong date -- likely this should have been
    # 2013-09-22 -- and then a concurrent 2-day stay in the male-only pathway before an unplanned move to (probably)
    # hospital. (2 issues)
    corrections.add("start_dt", 49017, "2013-09-22")
    corrections.add("move-vac", 29973)

    # cli_id 15943 had a 13-day placement in "Accommodation Based - Specialist Adult Services - Non-Pathway" during
    # their stay in the female-only pathway -- possibly because of being unable to stay in it temporarily.
    corrections.add("move-vac", 55736)

    # cli_id 16003 had two internal transfers mis-labelled as "Moved within Supported Housing (Same Pathway)"
    # (vac_id.isin([21788,987]))
    corrections.add("end_reason", 21788, "INTERNAL TRANSFER")
    corrections.add("end_reason", 987, "INTERNAL TRANSFER")

    # cli_id 19446 had a 28-day placement in substance misuse pathway level 1 while still in D&A abstinent accommodation.
    corrections.add("move-vac", 70249)

    # cli_id 20784 had two placements with the referral agency as "Test2", multiple moves backdated as if internal
    # transfers when they are to different services in differnt parthways, and (if that's all correct) then one
    # backdated incorrectly.
    # Checked original database. All 4 appear to be correctly backdated (16/06/2012). While they're different
    # providers/services/pathways, three are the same address(!), but the end reason is on the wrong placement.
    corrections.add("end_reason", 58116, "Moved to Local Authority Tenancy (Planned)")
    corrections.add("end_reason", 30580, "INTERNAL TRANSFER")

    # cli_id 21374 had an unusual set of placements, with a number in the male only and female only pathways, and a
    # difficult-to understand set of gaps.
    # Checked original database: complex client -- overlap likely genuine.

    ########################################################
    # Errors found from wrong order in Vacancy Filled Date
    ########################################################

    # cli_id 23195 appears to have had the end dates transposed between two vac_ids, which should be swapped back:
    corrections.add("end_dt", 50215, "2013-06-26")
    corrections.add("end_dt", 13495, "2013-06-09")

    # cli_id 23343 had an internal transfer mis-recorded as "Moved within Supported Housing (Same Pathway)" for
    # vac_id==26692
    corrections.add("end_reason", 26692, "INTERNAL TRANSFER")

    # cli_id 23488 appears to have had the end dates transposed between two vac_ids, which should be swapped back:
    # vac_id.isin([65045,19331])
    corrections.add("end_dt", 65045, "2013-03-11")
    corrections.add("end_dt", 19331, "2013-06-26")

    # cli_id 23805 had a 2-day placement in the male-only pathway (level 1) in 2011, during a stay in the young people's
    # pathway.
    corrections.add("move-vac", 26400)

    # cli_id 24418 moved within the young people pathway and then moved on to a lower support service during an overlap
    # with the previous service.
    corrections.add("end_dt", 25384, "2014-09-24")

    # cli_id 26621 had a 0-day placement in service 126 while staying in another service
    corrections.add("move-vac", 57075)

    # cli_id 27007 had a 36-day placement in L4 of the male-only pathway towards the end of a 766-day stay in L1 of the
    # female-only pathway. Would be a good one to look at.
    # Checked original database - appears to have been a short-lived move into L4 accommodation then quickly back to
    # L1. Almost immediately moved back but overlap related to notice preiod.
    corrections.add("move-vac", 65807, assumption=True)

    # cli_id moved from L4 of the male-only pathway to L1 and then quickly on into L2 but before the L4 placement had
    # been neded. Total overlap was about 13 days.
    corrections.add("end_dt", 66040, "2012-07-02")

    # cli_id 29329 had a 7-day placement in L4 during a longer placement in L3.
    corrections.add("move-vac", 11719)

    # cli_id 30463 likely wrong year entered for the end date for vac_id 63662, which looks like it should have been
    # 2015-06-03, but it's also possible that the wrong year was entered for start and end dates for vac_id 2157.
    # Checked original database. Notes discuss move taking place on 04/06/2015, so wrong end year.
    corrections.add("end_dt", 63662, "2015-06-03", assumption=True)

    # cli_id 31376 has a 33-day placement in the Male only pathway towards the end of a longer placement.
    corrections.add("move-vac", 7383)

    # cli_id 34558 has three placements in the middle of a really long placement from 2000-2020 in svc_id 1.
    for x in [34239, 14447, 69708]:
        corrections.add("move-vac", x)

    # cli_id 3331 had one placement incorrectly backdated (wrong year) for an internal transfer:
    corrections.add("start_dt", 63797, "2015-08-10")

    # cli_id 3771 had one placement incorrectly backdated (by one day) for an internal transfer:
    corrections.add("start_dt", 5302, "2019-07-24")

    # cli_id 5137 had one placement incorrectly backdated (by one day) for an internal transfer:
    corrections.add("start_dt", 33092, "2021-03-24")

    # cli_id 6231 had one placement incorrectly backdated (to just before previous end date) for an internal transfer:
    corrections.add("start_dt", 5148, "2011-10-17")

    # cli_id 7482 had sequential placements for internal transfers apart from one:
    corrections.add("start_dt", 62439, "2013-08-05")

    # cli_id 8211 had sequential placements for internal transfers apart from one:
    corrections.add("start_dt", 44340, "2022-03-10")

    # cli_id 10422 had one placement incorrectly backdated (by six months) for an internal transfer:
    corrections.add("start_dt", 11097, "2021-07-26")

    # cli_id 10732 had sequential placements for internal transfers apart from one:
    corrections.add("start_dt", 4256, "2012-12-10")

    # cli_id 13327 had one placement incorrectly backdated (by 5 days) for an internal transfer:
    corrections.add("start_dt", 8236, "2020-05-20")

    # cli_id 13946 had sequential placements for internal transfers apart from one:
    corrections.add("start_dt", 51777, "2013-01-02")

    # cli_id 17992 had one placement incorrectly backdated (by six days) for an internal transfer, setting to sequential:
    corrections.add("start_dt", 8109, "2015-12-18")

    # cli_id 18219 had a 0-day placement in another service, during a placement in a lower-level service.
    corrections.add("move-vac", 71328)

    # cli_id 19716 had one placement incorrectly backdated (by 7 days) for an internal transfer, setting to sequential:
    corrections.add("start_dt", 1684, "2017-10-26")

    # cli_id 20536 had one placement incorrectly backdated (to previous end date) for an internal transfer:
    corrections.add("start_dt", 7322, "2012-06-14")

    # cli_id 21995 had one placement incorrectly backdated (by 2 days) for an internal transfer, setting to sequential:
    corrections.add("start_dt", 19337, "2022-04-20")

    # cli_id 22020 had one placement incorrectly backdated for an internal transfer, but the unbackdating code will fix it.

    # cli_id 22885 had one placement incorrectly backdated (by 2 weeks) for an internal transfer:
    corrections.add("start_dt", 59776, "2014-06-30")

    # cli_id 24832 had one placement incorrectly backdated (by 9 days) for an internal transfer, setting to sequential:
    corrections.add("start_dt", 27342, "2015-12-08")

    # cli_id 25633 had one placement incorrectly backdated for an internal transfer, but the unbackdating code will fix it.
    # cli_id 26861 had one placement incorrectly backdated for an internal transfer, but the unbackdating code will fix it.

    # cli_id 28146 had a 34-day placement in another service, during a placement in a lower-level service.
    corrections.add("move-vac", 64063)

    # cli_id 28664 had a quick internal transfer (0 days) after moving to a new service with an overlap - make sequential:
    corrections.add("end_dt", 39437, "2013-02-04")

    # cli_id 30938 had an 18-day placement in a lower-level service, during a placement in a higher-level service.
    corrections.add("move-vac", 53015)

    # cli_id 32997 had one placement incorrectly backdated (to the end of the previous placement) for an internal transfer:
    corrections.add("start_dt", 14504, "2011-03-15")

    ######################################################################################
    # Errors found from multiple null Placement End Dates for the same people (o_cli_id)
    ######################################################################################

    # cli_id 8753 and 28886 either should not be linked together, or vac_id 66880 from 2009 in high support L2 should have
    # been closed down.
    # Checked original database and cli_ids should be linked. vac_id from 2009 should have been closed down
    # before 11/02/2010.
    corrections.add("end_dt", 66880, "2010-02-10")

    # For cli_id 8784 the issue identified above was also identified from this check.

    # cli_id 23105 appears to have moved into the Male Only Pathway as of 3 April, but the Mixed Pathway vacancy has not been
    # closed.
    # Checked original database: placement should have been closed down on 03/04/2025
    corrections.add("end_dt", 34200, "2025-04-03")

    # cli_id 29827 has the same concurrent ESA placement as cli_id 12923.
    # Checked original database: service type is not always accommodation.

    ####################################################################################
    # Errors found from duplicate Placement Start Dates for the same people (o_cli_id)
    ####################################################################################

    # o_cli_id==3919 had the wrong vac_id closed down: "INTERNAL TRANSFER" should be before a move to local authority tenancy
    corrections.add("end_dt", 2990, "2013-01-19")
    corrections.add("start_dt", 68456, "2013-01-19")
    corrections.add("end_dt", 68456, "2013-11-20")

    # o_cli_id==6128 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 27549, "2016-07-23")

    # o_cli_id==7702 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 26848, "2012-06-25")

    # o_cli_id==11908 had the wrong vac_id closed down: "INTERNAL TRANSFER" should be before a move to another service
    corrections.add("end_dt", 31548, "2012-11-19")
    corrections.add("start_dt", 20057, "2012-11-19")
    corrections.add("end_dt", 20057, "2012-11-21")

    # o_cli_id==12923 had a short placement during a longer placement in another service, affecting the order
    # and auto-unbackdating of internal transfers. Correcting manually:
    corrections.add("start_dt", 22407, "2023-03-30")

    # o_cli_id==16444 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 9132, "2020-12-17")

    # o_cli_id==17841 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 45386, "2020-12-15")

    # o_cli_id==20784 has a known issue (see vacancies_errors.py)

    # o_cli_id==21374 has konwn complex issues (see vacancies_errors.py)

    # o_cli_id==21691 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 10282, "2019-02-22")

    # o_cli_id==21907 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 26132, "2019-07-03")

    # o_cli_id==22161 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 51362, "2019-01-04")

    # o_cli_id==24221 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 52689, "2017-01-06")

    # o_cli_id==26621 had a short placement during a longer placement in another service, affecting the order
    # and auto-unbackdating of internal transfers. Correcting manually:
    corrections.add("start_dt", 10792, "2013-06-26")

    # o_cli_id==27157 had the wrong vac_id closed down: "INTERNAL TRANSFER" should be before a move to another service
    corrections.add("end_dt", 15542, "2013-05-05")
    corrections.add("start_dt", 17237, "2013-05-05")
    corrections.add("end_dt", 17237, "2013-06-26")

    # o_cli_id==28490 had a move between services backdated. Unbackdating:
    corrections.add("start_dt", 56573, "2012-12-10")

    # o_cli_id==29596 appears to have an unplanned move to a higher level of the pathway backdated.
    # Checked original database and this assumption is correct.
    corrections.add("start_dt", 10775, "2019-01-30")

    # o_cli_id==33321 had the wrong vac_id closed down: "INTERNAL TRANSFER" should be before a move to another service
    corrections.add("end_dt", 27041, "2013-04-22")
    corrections.add("start_dt", 25184, "2013-04-22")
    corrections.add("end_dt", 25184, "2013-06-26")

    #################################################################################
    # Errors found from negative gaps (overlaps) between placements where gap < -31
    #################################################################################

    # cli_id 1638 already identified above
    # cli_id 2664 already identified above

    # cli_id 2800 appears to have two concurrent placements that are not easily explained.
    # Checked original database. Appears to have been a late-entered move-out date or long
    # notice period.
    corrections.add("end_dt", 46535, "2015-12-09", assumption=True)

    # cli_id 2926 overlap appears to be genuine

    # cli_id 5368 overlap appears unusual as they supposedly moved to a LA tenancy
    # Checked original database. This appears to be a backdating issue relating to a decant
    # during refurbishment.
    corrections.add("end_dt", 26299, "2011-08-17", assumption=True)

    # cli_id 7379 already identified above
    # cli_id 7387 already identified above
    # cli_id 8490 already identified above
    # cli_id 8784 already identified above

    # cli_id 10515 has inconsistent overlapping placements in different services,
    # Emergency L1 and Parent & Baby, overlapping by 104 days but doesn't look like
    # a backdating issue.
    # Checked original database. The previous placement should have been closed down on 07/09/2009:
    # wrong year entered.
    corrections.add("end_dt", 13862, "2009-09-07")

    # cli_id 12573 already identified above
    # cli_id 12923 already identified above
    # cli_id 14445 already identified above
    # cli_id 15517 already identified above
    # cli_id 15943 already identified above

    # cli_id 16337 overlap appears to be genuine: eviction from one service but
    # accommodated in another service.

    # cli_id 19446 already identified above
    # cli_id 20784 already identified above

    # cli_id 22324 has an unusual overlap that appears inconsistent. Could be an
    # error with backdating.
    # Checked original database. Wrong year entered as end date. Should have been 2010.
    corrections.add("end_dt", 35097, "2010-11-28")

    # cli_id 23805 already identified above

    # cli_id 24201 has an unusual long overlap. Possible data entry error?
    # Checked original database. Previous vacancy appears to have been closed down late.
    corrections.add("end_dt", 63190, "05/06/2017")

    # cli_id 26380 overlap appears genuine.

    # cli_id 26621 already identified above
    # cli_id 27007 already identified above

    # cli_id 27266 overlap appears genuine.
    # cli_id 27571 overlap appears genuine.
    # cli_id 27807 overlap appears genuine.
    # cli_id 28999 overlap appears genuine.
    # cli_id 29201 overlap appears genuine.

    # cli_id 29329 already identified above
    # cli_id 29596 already identified above
    # cli_id 30463 already identified above
    # cli_id 31376 already identified above

    # cli_id 33702 overlap appears genuine.

    # cli_id 34558 already identified above

    #################################################################################
    # Errors found from placements contained within other placemetns where dur != 0
    #################################################################################

    # cli_id 6482 had a move with an overlap then a quick move onto another place.
    # Setting the end date for the previous placement to the start date of the overlap.
    corrections.add("end_dt", 45172, "2012-11-16")

    # cli_id 11908 had a move within pathways backdated. Unbackdating:
    corrections.add("start_dt", 53108, "2012-11-21")

    # cli_id 21321 had an overlap then a quick move. Unoverlapping:
    corrections.add("end_dt", 65952, "2014-10-29")

    # cli_id 21374 had a transfer between services backdated. Unbackdating:
    corrections.add("start_dt", 57922, "2014-03-23")

    # cli_id 23457 had a normal overlap. Unoverlapping:
    corrections.add("end_dt", 16782, "2009-12-11")

    # cli_id 27807 had a normla overlap. Unoverlapping:
    corrections.add("end_dt", 35154, "2010-09-14")

    ##############################################################
    # Errors found from gap > 50 days after an INTERNAL TRANSFER
    ##############################################################

    # cli_id 2433 gap unclear (70 days)
    # cli_id 5441 gap unclear (98 days)

    # cli_id 7398 appears to have a move backdated to the wrong year
    corrections.add("start_dt", 44040, "2021-05-28")

    # cli_id 8011 gap unclear (327 days) - could be wrong placement closed
    # cli_id 9234 gap unclear (181 days) - could be wrong placement closed
    # cli_id 9360 gap unclear (330 days)
    # cli_id 10000 gap unclear (422 days)
    # cli_id 10763 gap unclear (164 days)

    # cli_id 12078 appears to have the wrong placement closed down
    corrections.add("start_dt", 7458, "2021-09-27")
    corrections.add("end_dt", 7458, "2021-10-08")
    corrections.add("start_dt", 14480, "2021-08-28")
    corrections.add("end_dt", 14480, "2021-09-27")

    # cli_id 12405 gap unclear (867 days). Perhaps svc_id==105 used the
    # term INTERNAL TRANSFER for a non-pathways service?

    # cli_id 14509 gap unclear (156 days). Also relates to svc_id==105

    # cli_id 18926 went to both the female-only and male-only pathways
    # Checked original database -- the male-only service moved from the mixed to male-only pathway
    # since the previous placement date instead of being closed down and a new one created.

    # cli_id 21624 gap unclear (62 days) - could be a data entry error
    # Checked original database, data entry error apparent. Change start date to same day as previous.

    # cli_id 23444 appears to have the wrong placement closed down
    corrections.add("start_dt", 9302, "2020-01-22")
    corrections.add("end_dt", 9302, "2020-09-03")
    corrections.add("start_dt", 48006, "2020-09-03")
    corrections.add("end_dt", 48006, "2020-09-09")

    # cli_id 23677 gap unclear (277 days) - could be a data entry error
    # Checked original database -- appears that the wrong placement was closed down.
    # TODO: Check which one.

    # cli_id 24194 gap following ESA placement but svc_id!=105, appears to be accommodation, not FS.
    # The client has 4 placements at two places, but both transitioned between different
    # services while the person was still there and the placements were backdated. One should be 11/9/12 to
    # 15/4/13 and the next one 15/4/13 to 02/02/2015
    corrections.add("svc_id", 71606, "105")

    # cli_id 29058 gap unclear (730 days) - could be a data entry error
    # Checked -- genuine gap. Looks like a transfer to non-HSR accommodation then back in...

    # cli_id 31304 appears to have the wrong placement closed down
    # and also a likely data entry backdating to the wrong year (should this be 2018?)
    # Checked original database. This should be backdated to 2018. Assumptions correct.
    corrections.add("start_dt", 13664, "2018-02-20", assumption=True)
    corrections.add("end_dt", 13664, "2018-04-16", assumption=True)
    corrections.add("start_dt", 39452, "2018-01-23", assumption=True)
    corrections.add("end_dt", 39452, "2018-02-20", assumption=True)
    corrections.add("start_dt", 69608, "2018-04-16", assumption=True)

    # cli_id 31474 gap unclear (168 days) - could be a data entry error
    # Checked original database. Different service no longer part of female-only pathway was part of it
    # at the time and covers the gap.

    # cli_id 32253 gap unclear (99 days) - could be a data entry error
    # Checked original database. No gap, but placement was not recorded (internal move) as OABS include
    # charity guardianship project -- need to remove this from OABs definition.

    return corrections
