import argparse
import datetime as dt
import pandas as pd


def parse_args():
    args_parser = argparse.ArgumentParser(description="Entry point of this project", formatter_class=argparse.RawTextHelpFormatter)
    args_parser.add_argument("-w", "--switch", type=str,
                             help="""use this to decide which parts to run, available options = {
        'd': download,
        't': translate to sqlite/tsdb
        'c': check values
        }""")
    args_parser.add_argument("-t", "--type", type=str, default="",
                             help="""available options = {'cal', 'md_wds', 'md_tsdb', 'em01', 'cm01', 'posc', 'pose', 'stock', 'basis'}""")
    args_parser.add_argument("-m", "--mode", type=str, choices=("o", "a"), help="""run mode, available options = {'o', 'a'}""")
    args_parser.add_argument("-b", "--bgn", type=str, help="""begin date""")
    args_parser.add_argument("-s", "--stp", type=str, help="""stop  date""")
    args_parser.add_argument("-bs", "--bgnShift", type=int, default=30, help="""begin date shift when append calendar, must > 0""")
    args_parser.add_argument("-ss", "--stpShift", type=int, default=60, help="""stop  date shift when append calendar, must > 0""")
    args_parser.add_argument("-vs", "--values", type=str, default="close,volume,amount,oi", help="""values to be checked, separate by ','""")
    args = args_parser.parse_args()

    _switch = args.switch.upper()
    _data_type = args.type.upper()
    _run_mode = args.mode.upper()
    _bgn_date, _stp_date = args.bgn, args.stp
    _bgn_shift, _stp_shift = args.bgnShift, args.stpShift
    if _stp_date is None:
        _stp_date = (dt.datetime.strptime(_bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    _values = args.values.split(",")
    return _switch, _data_type, (_run_mode, _bgn_date, _stp_date), (_bgn_shift, _stp_shift), _values


if __name__ == "__main__":
    switch, data_type, t3, t2, check_values = parse_args()
    run_mode, bgn_date, stp_date = t3
    if switch == "D":
        from project_setup import (global_config, calendar_path, futures_instru_info_path,
                                   futures_by_date_dir)
        from skyrim.whiterun import CCalendar, CInstrumentInfoTable

        # shared variables and tools
        calendar = CCalendar(calendar_path)
        instru_info_table_w = CInstrumentInfoTable(futures_instru_info_path, t_index_label="windCode", t_type="CSV")
        instru_info_table_i = CInstrumentInfoTable(futures_instru_info_path, t_index_label="instrumentId", t_type="CSV")
        universe = instru_info_table_w.get_universe()
        wds_account = dict(
            host=global_config["account"]["wds"]["host"],
            user=global_config["account"]["wds"]["user"],
            passwd=global_config["account"]["wds"]["passwd"],
            database=global_config["account"]["wds"]["database"],
        )

        if data_type == "CAL":
            from DownloadEngineWDS import CDownloadEngineWDS
            from ManagerCalendar import download_calendar

            append_bgn_shift, append_stp_shift = t2
            download_engine_wds = CDownloadEngineWDS(**wds_account)
            download_calendar(calendar_path, run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date,
                              append_bgn_shift=append_bgn_shift, append_stp_shift=append_stp_shift,
                              download_engine_wds=download_engine_wds)
        elif data_type == "MD_WDS":
            from DownloadEngineWDS import CDownloadEngineWDS
            from ManagerDailyIncrementDataWDS import CManagerDailyIncrementDataMdWDS

            download_engine_wds = CDownloadEngineWDS(**wds_account)
            download_values = [
                "S_INFO_WINDCODE",
                "S_DQ_SETTLE", "S_DQ_PRESETTLE",
                "S_DQ_OPEN", "S_DQ_HIGH", "S_DQ_LOW", "S_DQ_CLOSE",
                "S_DQ_VOLUME", "S_DQ_AMOUNT", "S_DQ_OI",
                "S_DQ_CHANGE",
            ]
            rename_mapper = {
                "S_INFO_WINDCODE": "wind_code",
                "S_DQ_SETTLE": "settle",
                "S_DQ_PRESETTLE": "presettle",
                "S_DQ_OPEN": "open",
                "S_DQ_HIGH": "high",
                "S_DQ_LOW": "low",
                "S_DQ_CLOSE": "close",
                "S_DQ_VOLUME": "volume",
                "S_DQ_AMOUNT": "amount",
                "S_DQ_OI": "oi",
                "S_DQ_CHANGE": "change",

            }
            mgr_download = CManagerDailyIncrementDataMdWDS(
                download_values=download_values, rename_mapper=rename_mapper,
                download_engine_wds=download_engine_wds,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
        elif data_type == "MD_TSDB":
            from TSDBTranslator2.translator import CTSDBReader
            from ManagerDailyIncrementDataTSDB import CManagerDailyIncrementDataMdTSDB

            download_values = [
                "open", "high", "low", "close", "settle",
                "preclose", "presettle",
                "vol", "amount", "oi"
            ]
            download_engine_tsdb = CTSDBReader(t_tsdb_path=global_config["TSDB"]["server_path"])
            mgr_download = CManagerDailyIncrementDataMdTSDB(
                download_values=download_values, instru_info_table=instru_info_table_i,
                download_engine_tsdb=download_engine_tsdb,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
            mgr_download.patch(bgn_date, stp_date)

        elif data_type == "CM01":
            from TSDBTranslator2.translator import CTSDBReader
            from ManagerDailyIncrementDataTSDB import CManagerDailyIncrementDataCM01TSDB

            download_values = [
                "open", "high", "low", "close",
                "vol", "amount", "oi",
                "daily_open", "daily_high", "daily_low",
                "preclose", "preoi",
            ]
            download_engine_tsdb = CTSDBReader(t_tsdb_path=global_config["TSDB"]["server_path"])
            mgr_download = CManagerDailyIncrementDataCM01TSDB(
                download_values=download_values, instru_info_table=instru_info_table_i,
                download_engine_tsdb=download_engine_tsdb,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
        elif data_type == "EM01":
            from TSDBTranslator2.translator import CTSDBReader
            from ManagerDailyIncrementDataTSDB import CManagerDailyIncrementDataEM01TSDB

            download_values = [
                "open", "high", "low", "close",
                "vol", "amount", "oi",
                "daily_open", "daily_high", "daily_low",
                "preclose", "preoi",
            ]
            download_engine_tsdb = CTSDBReader(t_tsdb_path=global_config["TSDB"]["server_path"])
            mgr_download = CManagerDailyIncrementDataEM01TSDB(
                download_values=download_values, instru_info_table=instru_info_table_i,
                download_engine_tsdb=download_engine_tsdb,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
        elif data_type == "POSC":
            from DownloadEngineWDS import CDownloadEngineWDS
            from ManagerDailyIncrementDataWDS import CManagerDailyIncrementDataPositionWDS

            download_engine_wds = CDownloadEngineWDS(**wds_account)
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
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
        elif data_type == "POSE":
            from DownloadEngineWDS import CDownloadEngineWDS
            from ManagerDailyIncrementDataWDS import CManagerDailyIncrementDataPositionWDS

            download_engine_wds = CDownloadEngineWDS(**wds_account)
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
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
        elif data_type == "STOCK":
            from DownloadEngineWDS import CDownloadEngineWDS
            from ManagerDailyIncrementDataWDS import CManagerDailyIncrementDataStockWDS

            download_engine_wds = CDownloadEngineWDS(**wds_account)
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
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
        elif data_type == "BASIS":
            from DownloadEngineWAPI import CDownloadEngineWAPI
            from ManagerDailyIncrementDataWAPI import CManagerDailyIncrementDataBasisWAPI

            download_engine_wapi = CDownloadEngineWAPI(instruments=universe)
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
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
    elif switch == "T":
        pass
    elif switch == "C":
        from project_setup import calendar_path, futures_dir, futures_md_wds_db_name, futures_md_tsdb_db_name
        from Checks import check_tsdb_and_wds
        from skyrim.whiterun import CCalendar

        calendar = CCalendar(calendar_path)
        check_tsdb_and_wds(
            check_values=check_values, bgn_date=bgn_date, stp_date=stp_date,
            futures_dir=futures_dir, futures_md_wds_db_name=futures_md_wds_db_name, futures_md_tsdb_db_name=futures_md_tsdb_db_name,
            calendar=calendar, verbose=True
        )
