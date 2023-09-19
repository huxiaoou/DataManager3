import argparse
import datetime as dt
import pandas as pd


def parse_args():
    args_parser = argparse.ArgumentParser(description="Entry point of this project", formatter_class=argparse.RawTextHelpFormatter)
    args_parser.add_argument("-w", "--switch", type=str,
                             help="""use this to decide which parts to run, available options = {
        'd': download,
        't': translate to sqlite or tsdb
        }""")
    args_parser.add_argument("-t", "--type", type=str, default="",
                             help="""available options = {'cal', 'md_wds', 'md_tsdb', 'em01', 'cm01', 'posc', 'pose', 'stock', 'basis'}""")
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
        from project_setup import (global_config, calendar_path, futures_instru_info_path,
                                   futures_by_date_dir)
        from DownloadEngineWDS import CDownloadEngineWDS
        from DownloadEngineWAPI import CDownloadEngineWAPI
        from skyrim.whiterun import CCalendar, CInstrumentInfoTable

        # shared variables and tools
        calendar = CCalendar(calendar_path)
        instru_info_table = CInstrumentInfoTable(futures_instru_info_path, t_index_label="windCode", t_type="CSV")
        universe = instru_info_table.get_universe()
        download_engine_wds = CDownloadEngineWDS(
            host=global_config["account"]["wds"]["host"],
            user=global_config["account"]["wds"]["user"],
            passwd=global_config["account"]["wds"]["passwd"],
            database=global_config["account"]["wds"]["database"],
        )
        download_engine_wapi = CDownloadEngineWAPI(instruments=universe)

        if data_type == "CAL":
            from ManagerCalendar import download_calendar

            append_bgn_shift, append_stp_shift = t2
            download_calendar(calendar_path, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                              append_bgn_shift=append_bgn_shift, append_stp_shift=append_stp_shift,
                              download_engine_wds=download_engine_wds)
        elif data_type == "MD_WDS":
            from ManagerDailyIncrementData import CManagerDailyIncrementDataMdWDS

            download_values = [
                "S_INFO_WINDCODE",
                "S_DQ_SETTLE", "S_DQ_PRESETTLE",
                "S_DQ_OPEN", "S_DQ_HIGH", "S_DQ_LOW", "S_DQ_CLOSE",
                "S_DQ_VOLUME", "S_DQ_AMOUNT", "S_DQ_OI",
                "S_DQ_CHANGE",
            ]
            mgr_download = CManagerDailyIncrementDataMdWDS(
                download_values=download_values,
                download_engine_wds=download_engine_wds,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date)
        elif data_type == "MD_TSDB":
            pass
        elif data_type == "CM01":
            pass
        elif data_type == "EM01":
            pass
        elif data_type == "POSC":
            from ManagerDailyIncrementData import CManagerDailyIncrementDataPositionWDS

            download_values = [
                "FS_INFO_TYPE",  # "rnk_type", primary key
                "FS_INFO_MEMBERNAME",  # "member", primary key
                "FS_INFO_POSITIONSNUM",  # "pos_qty"
                "FS_INFO_RANK",  # "pos_rnk"
                "S_OI_POSITIONSNUMC",  # "pos_dlt"
                "S_INFO_WINDCODE",
            ]
            rename_mapper = {
                "FS_INFO_TYPE": "type",
                "FS_INFO_MEMBERNAME": "member_chs",
                "FS_INFO_POSITIONSNUM": "pos",
                "FS_INFO_RANK": "rank",
                "S_OI_POSITIONSNUMC": "delta",
                "S_INFO_WINDCODE": "wind_code",
            }
            mgr_download = CManagerDailyIncrementDataPositionWDS(
                download_values=download_values, rename_mapper=rename_mapper, futures_type="C",
                download_engine_wds=download_engine_wds,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=True)
        elif data_type == "POSE":
            from ManagerDailyIncrementData import CManagerDailyIncrementDataPositionWDS

            download_values = [
                "FS_INFO_TYPE",  # "rnk_type", primary key
                "FS_INFO_MEMBERNAME",  # "member", primary key
                "FS_INFO_POSITIONSNUM",  # "pos_qty"
                "FS_INFO_RANK",  # "pos_rnk"
                "S_OI_POSITIONSNUMC",  # "pos_dlt"
                "S_INFO_WINDCODE",
            ]
            rename_mapper = {
                "FS_INFO_TYPE": "type",
                "FS_INFO_MEMBERNAME": "member_chs",
                "FS_INFO_POSITIONSNUM": "pos",
                "FS_INFO_RANK": "rank",
                "S_OI_POSITIONSNUMC": "delta",
                "S_INFO_WINDCODE": "wind_code",
            }
            mgr_download = CManagerDailyIncrementDataPositionWDS(
                download_values=download_values, rename_mapper=rename_mapper, futures_type="E",
                download_engine_wds=download_engine_wds,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=True)
        elif data_type == "STOCK":
            from project_setup import global_config
            from ManagerDailyIncrementData import CManagerDailyIncrementDataStockWDS

            download_values = [
                "FS_INFO_SCNAME",
                "IN_STOCK_TOTAL",
                "IN_STOCK",
                "AVAILABLE_IN_STOCK",
            ]
            rename_mapper = {
                "IN_STOCK_TOTAL": "in_stock_total",
                "IN_STOCK": "in_stock",
                "AVAILABLE_IN_STOCK": "available_in_stock",
            }
            header_df = pd.DataFrame({"wind_code": universe})
            id_mapper = global_config["futures"]["mapper_name_chs_to_eng"]
            mgr_download = CManagerDailyIncrementDataStockWDS(
                download_values=download_values, header_df=header_df,
                id_mapper=id_mapper, rename_mapper=rename_mapper, drop_cols=["FS_INFO_SCNAME"],
                download_engine_wds=download_engine_wds,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date)
        elif data_type == "BASIS":
            from ManagerDailyIncrementData import CManagerDailyIncrementDataBasisWAPI

            download_values = ["anal_basis", "anal_basispercent2"]
            rename_mapper = {
                "anal_basis": "basis",
                "anal_basispercent2": "basis_rate",
            }
            mgr_download = CManagerDailyIncrementDataBasisWAPI(
                download_values=download_values, rename_mapper=rename_mapper,
                download_engine_wapi=download_engine_wapi,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date)
