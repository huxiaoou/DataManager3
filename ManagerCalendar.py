import datetime as dt
from DownloadEngineWDS import CDownloadEngineWDS
from skyrim.whiterun import CCalendarBase


def download_calendar(calendar_path: str, run_mode: str, bgn_date: str, stp_date: str,
                      append_bgn_shift: int,
                      append_stp_shift: int,
                      download_engine_wds: CDownloadEngineWDS
                      ):
    """

    :param calendar_path:
    :param run_mode:
    :param bgn_date:
    :param stp_date:
    :param append_bgn_shift:
    :param append_stp_shift:
    :param download_engine_wds:
    :return:
    """

    calendar: CCalendarBase = CCalendarBase(calendar_path)
    if run_mode == "O":
        update_df = download_engine_wds.download_futures_calendar(bgn_date, stp_date, exchange_id="SHFE")
        calendar.update_all(update_df)
    elif run_mode == "A":
        if (append_bgn_shift > 0) and (append_stp_shift > 0):
            bgn_tp = dt.datetime.strptime(bgn_date, "%Y%m%d")
            shift_bgn_date = (bgn_tp - dt.timedelta(days=append_bgn_shift)).strftime("%Y%m%d")
            shift_stp_date = (bgn_tp + dt.timedelta(days=append_stp_shift)).strftime("%Y%m%d")
        else:
            shift_bgn_date = bgn_date
            shift_stp_date = stp_date
        update_df = download_engine_wds.download_futures_calendar(shift_bgn_date, shift_stp_date, exchange_id="SHFE")
        calendar.update_increment(update_df)
    else:
        print("Not a right mode, please check again. Available options = ['o', 'overwrite', 'a', 'append']")
    download_engine_wds.close()
    return 0
