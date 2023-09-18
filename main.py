import argparse
import datetime as dt
from ManagerCalendar import download_calendar
from DownloadEngineWDS import CDownloadEngineWDS


def parse_args():
    args_parser = argparse.ArgumentParser(description="Entry point of this project", formatter_class=argparse.RawTextHelpFormatter)
    args_parser.add_argument("-w", "--switch", type=str,
                             help="""use this to decide which parts to run, available options = {
        'd': download,
        't': translate to sqlite or tsdb
        }""")
    args_parser.add_argument("-t", "--type", type=str, default="",
                             help="""available options = {'cal', ''}""")
    args_parser.add_argument("-m", "--mode", type=str, choices=("o", "a"), help="""run mode, available options = {'o', 'a'}""")
    args_parser.add_argument("-b", "--bgn", type=str, help="""begin date""")
    args_parser.add_argument("-s", "--stp", type=str, help="""stop  date""")
    args_parser.add_argument("-bs", "--bgnShift", type=int, default=0, help="""begin date shift when append calendar, must > 0""")
    args_parser.add_argument("-ss", "--stpShift", type=int, default=0, help="""stop  date shift when append calendar, must > 0""")
    args = args_parser.parse_args()

    _switch = args.switch.upper()
    _data_type = args.type.upper()
    _run_mode = args.mode.upper()
    _bgn_date, _stp_date = args.bgn, args.stp
    _bgn_shift, _stp_shift = args.bgnShift, args.stpShift
    if _stp_date is None:
        _stp_date = (dt.datetime.strptime(_bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    return _switch, _data_type, (_run_mode, _bgn_date, _stp_date), (_bgn_shift, _stp_shift)


if __name__ == "__main__":
    switch, data_type, t3, t2 = parse_args()
    run_mode, bgn_date, stp_date = t3
    if switch == "D":
        from project_setup import global_config

        download_engine_wds = CDownloadEngineWDS(
            host=global_config["account"]["host"],
            user=global_config["account"]["user"],
            passwd=global_config["account"]["passwd"],
            database=global_config["account"]["database"],
        )
        if data_type == "CAL":
            from project_setup import calendar_path
            append_bgn_shift, append_stp_shift = t2
            download_calendar(calendar_path, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                              append_bgn_shift=append_bgn_shift, append_stp_shift=append_stp_shift,
                              download_engine_wds=download_engine_wds)
