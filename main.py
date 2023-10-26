import argparse
import datetime as dt


def parse_args():
    args_parser = argparse.ArgumentParser(description="Entry point of this project", formatter_class=argparse.RawTextHelpFormatter)
    args_parser.add_argument("-w", "--switch", type=str,
                             help="""use this to decide which parts to run, available options = {
        'd': download,
        's': sync between local and server
        'c': check values
        't': translate to sqlite/tsdb
        }""")
    args_parser.add_argument("-t", "--type", type=str, default="",
                             help="""available options = {'cal', 'md_wds', 'md_tsdb', 'em01', 'cm01', 'posc', 'pose', 'stock', 'basis', 'rf', 'rd', 'nf', 'nd'}""")
    args_parser.add_argument("-m", "--mode", type=str, choices=("o", "a"), help="""run mode, available options = {'o', 'a'}""")
    args_parser.add_argument("-b", "--bgn", type=str, help="""begin date""")
    args_parser.add_argument("-s", "--stp", type=str, help="""stop  date""")
    args_parser.add_argument("-bs", "--bgnShift", type=int, default=30, help="""begin date shift when append calendar, must > 0""")
    args_parser.add_argument("-ss", "--stpShift", type=int, default=60, help="""stop  date shift when append calendar, must > 0""")
    args_parser.add_argument("-vs", "--values", type=str, help="""values to be checked, separate by ','""")
    args = args_parser.parse_args()

    _switch = args.switch.upper()
    _data_type = args.type.upper()
    _run_mode = None if _switch in ["S"] else args.mode.upper()  # no necessary if switch is "S"
    _bgn_date, _stp_date = args.bgn, args.stp
    _bgn_shift, _stp_shift = args.bgnShift, args.stpShift
    if _stp_date is None:
        _stp_date = (dt.datetime.strptime(_bgn_date, "%Y%m%d") + dt.timedelta(days=1)).strftime("%Y%m%d")
    _values = args.values.split(",") if _switch in ["S", "C"] else []
    return _switch, _data_type, (_run_mode, _bgn_date, _stp_date), (_bgn_shift, _stp_shift), _values


if __name__ == "__main__":
    import pandas as pd
    from skyrim.whiterun import SetFontYellow

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
            download_engine_tsdb = CTSDBReader(t_tsdb_path=global_config["TSDB"]["path"]["public"])
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
            download_engine_tsdb = CTSDBReader(t_tsdb_path=global_config["TSDB"]["path"]["public"])
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
            download_engine_tsdb = CTSDBReader(t_tsdb_path=global_config["TSDB"]["path"]["public"])
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

            # for commodity
            download_engine_wapi = CDownloadEngineWAPI(instruments=universe)
            download_values = ["anal_basis", "anal_basispercent2"]
            rename_mapper = {
                "anal_basis": "basis",
                "anal_basispercent2": "basis_rate",
            }
            mgr_download = CManagerDailyIncrementDataBasisWAPI(
                download_values=download_values, rename_mapper=rename_mapper,
                exchange_filter=["SHF", "INE", "DCE", "CZC", "GFE"], file_name_format="basis.{}.csv.gz",
                download_engine_wapi=download_engine_wapi,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])

            # for financial
            download_engine_wapi = CDownloadEngineWAPI(instruments=["IC.CFE", "IF.CFE", "IH.CFE", "IM.CFE"])
            download_values = ["if_basis", "anal_basispercent", "anal_basisannualyield"]
            rename_mapper = {
                "if_basis": "basis",
                "anal_basispercent": "basis_rate",
                "anal_basisannualyield": "basis_rate_annual",
            }
            mgr_download = CManagerDailyIncrementDataBasisWAPI(
                download_values=download_values, rename_mapper=rename_mapper,
                exchange_filter=["CFE"], file_name_format="basis_cfe.{}.csv.gz",
                download_engine_wapi=download_engine_wapi,
                data_save_dir=futures_by_date_dir, calendar=calendar
            )
            mgr_download.main(run_mode, bgn_date, stp_date, verbose=run_mode in ["A"])
    elif switch == "S":
        import platform

        this_platform = platform.system().upper()
        if this_platform == "WINDOWS":
            from project_setup import calendar_path, futures_by_date_dir, global_config
            from skyrim.whiterun import CCalendar
            from CheckAndSync import CAgentSyncDailyIncrement

            sync_values_interpreter = {
                "download": {
                    "md": "md_tsdb.{}.csv.gz",
                    "cm01": "cm01_tsdb.{}.csv.gz",
                    "em01": "em01_tsdb.{}.csv.gz",
                },
                "upload": {
                    "basis": "basis.{}.csv.gz",
                    "basis_cfe": "basis_cfe.{}.csv.gz",
                }
            }
            calendar = CCalendar(calendar_path)
            agent_sync = CAgentSyncDailyIncrement(
                remote_root_dir="/home/huxo/Deploy/Data/Futures/by_date",
                local_root_dir=futures_by_date_dir,
                hostname=global_config["account"]["Server-GH2"]["host"],
                username=global_config["account"]["Server-GH2"]["user"],
            )
            download_file_format = list(filter(lambda _: _ is not None, [sync_values_interpreter["download"].get(v, None) for v in check_values]))
            upload_file_format = list(filter(lambda _: _ is not None, [sync_values_interpreter["upload"].get(v, None) for v in check_values]))
            agent_sync.download_by_dates(download_file_format, bgn_date, stp_date, calendar, verbose=True)
            agent_sync.upload_by_dates(upload_file_format, bgn_date, stp_date, calendar, verbose=True)
            agent_sync.close()
    elif switch == "C":
        from project_setup import calendar_path, futures_by_date_dir, futures_md_wds_db_name, futures_md_tsdb_db_name
        from CheckAndSync import check_tsdb_and_wds
        from skyrim.whiterun import CCalendar

        calendar = CCalendar(calendar_path)
        check_tsdb_and_wds(
            check_values=check_values, bgn_date=bgn_date, stp_date=stp_date,
            futures_by_date_dir=futures_by_date_dir, futures_md_wds_db_name=futures_md_wds_db_name, futures_md_tsdb_db_name=futures_md_tsdb_db_name,
            calendar=calendar, verbose=run_mode in ["A"]  # to print details if no error
        )
    elif switch == "T":
        from project_setup import futures_dir, futures_by_date_dir, calendar_path, db_structs
        from ManagerDailyIncrementData import CManagerDailyIncrementData
        from skyrim.whiterun import CCalendar
        from skyrim.falkreath import CLib1Tab1, CTable

        calendar = CCalendar(calendar_path)
        if data_type == "MD_WDS":
            from project_setup import futures_md_wds_db_name

            mgr_download = CManagerDailyIncrementData("md_wds.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_md_wds_db_name]["CTable"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_md_wds_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )
        elif data_type == "MD_TSDB":
            from project_setup import futures_md_tsdb_db_name

            mgr_download = CManagerDailyIncrementData("md_tsdb.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_md_tsdb_db_name]["CTable"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_md_tsdb_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )
        elif data_type == "CM01":
            from project_setup import futures_cm01_db_name

            mgr_download = CManagerDailyIncrementData("cm01_tsdb.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_cm01_db_name]["CTable"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_cm01_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )
        elif data_type == "EM01":
            from project_setup import futures_em01_db_name

            mgr_download = CManagerDailyIncrementData("em01_tsdb.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_em01_db_name]["CTable"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_em01_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )
        elif data_type == "POSC":
            from project_setup import futures_positions_c_db_name

            mgr_download = CManagerDailyIncrementData("positions.C.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_positions_c_db_name]["CTable"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_positions_c_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )
        elif data_type == "POSE":
            from project_setup import futures_positions_e_db_name

            mgr_download = CManagerDailyIncrementData("positions.E.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_positions_e_db_name]["CTable"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_positions_e_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )
        elif data_type == "STOCK":
            from project_setup import futures_fundamental_db_name
            import platform

            # to sqlite
            mgr_download = CManagerDailyIncrementData("stock.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_fundamental_db_name]["CTableStock"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_fundamental_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )

            # to tsdb
            this_platform = platform.system().upper()
            if this_platform == "LINUX":
                from project_setup import global_config
                from TranslatorFundamental import translate_fundamental_from_csv_to_tsdb

                iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
                end_date = iter_dates[-1]
                translate_fundamental_from_csv_to_tsdb(
                    factor_lbl="stock",
                    src_csv_db_path=futures_by_date_dir, tsdb_table_name=global_config["TSDB"]["tables"]["fund"],  # "huxo.fundamental",
                    run_mode=run_mode, bgn_date=bgn_date, end_date=end_date, stp_date=stp_date,
                    # custom_ts_db_path="/home/huxo/Deploy/Data/TSDB/", futures_md_ts_db_path="/var/TSDB/futures",
                    custom_ts_db_path=global_config["TSDB"]["path"]["private"], futures_md_ts_db_path=global_config["TSDB"]["path"]["public"],
                    tsdb_bgn_date=global_config["TSDB"]["bgn_date"],  # "20120101"
                )
            else:
                print(f"... {SetFontYellow('Warning')}! When translating STOCK data from CSV to TSDB")
                print(f"... This plat form is = {this_platform}, but it is expected to be LINUX")
        elif data_type == "BASIS":
            from project_setup import futures_fundamental_db_name
            import platform

            # to sqlite
            mgr_download = CManagerDailyIncrementData("basis.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_fundamental_db_name]["CTableBasis"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_fundamental_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )

            mgr_download = CManagerDailyIncrementData("basis_cfe.{}.csv.gz", futures_by_date_dir, calendar)
            table = CTable(db_structs[futures_fundamental_db_name]["CTableBasisCFE"])
            mgr_download.to_sqlite_database(
                dst_db_save_dir=futures_dir,
                dst_db_struct=CLib1Tab1(futures_fundamental_db_name, table),
                run_mode=run_mode, bgn_date=bgn_date, stp_date=stp_date
            )

            # to tsdb
            this_platform = platform.system().upper()
            if this_platform == "LINUX":
                from project_setup import global_config
                from TranslatorFundamental import translate_fundamental_from_csv_to_tsdb

                iter_dates = calendar.get_iter_list(bgn_date, stp_date, True)
                end_date = iter_dates[-1]
                translate_fundamental_from_csv_to_tsdb(
                    factor_lbl="basis",
                    src_csv_db_path=futures_by_date_dir, tsdb_table_name=global_config["TSDB"]["tables"]["fund"],  # "huxo.fundamental",
                    run_mode=run_mode, bgn_date=bgn_date, end_date=end_date, stp_date=stp_date,
                    # custom_ts_db_path="/home/huxo/Deploy/Data/TSDB/", futures_md_ts_db_path="/var/TSDB/futures",
                    custom_ts_db_path=global_config["TSDB"]["path"]["private"], futures_md_ts_db_path=global_config["TSDB"]["path"]["public"],
                    tsdb_bgn_date=global_config["TSDB"]["bgn_date"],  # "20120101"
                )
            else:
                print(f"... {SetFontYellow('Warning')}! When translating BASIS data from CSV to TSDB")
                print(f"... This plat form is = {this_platform}, but it is expected to be LINUX")
        elif data_type in ["RF", "RD", "NF", "ND"]:
            from project_setup import tsdb_private_path, tsdb_public_path
            from project_setup import signals_portfolios_dir, futures_instru_info_path, futures_by_instrument_dir
            from TranslatorFactor import translate_signal_from_sql_to_tsdb
            from skyrim.whiterun import CInstrumentInfoTable

            instru_info_tab = CInstrumentInfoTable(futures_instru_info_path, "windCode", "CSV")
            concerned_universe = instru_info_tab.get_universe()
            end_date = (dt.datetime.strptime(stp_date, "%Y%m%d") - dt.timedelta(days=1)).strftime("%Y%m%d")

            # factor relative settings
            tsdb_bgn_date = "20120101"
            factor_value_columns = ["instrument", "value"]
            tsdb_table_name = "huxo.portfolio"
            values_rename_mapper = {"value": data_type}

            translate_signal_from_sql_to_tsdb(
                factor_lbl=data_type, factor_sql_db_dir=signals_portfolios_dir, tsdb_table_name=tsdb_table_name,
                run_mode=run_mode, bgn_date=bgn_date, end_date=end_date, stp_date=stp_date,
                concerned_universe=concerned_universe, by_instru_dir=futures_by_instrument_dir,
                tsdb_private_path=tsdb_private_path, tsdb_public_path=tsdb_public_path,
                factor_value_columns=factor_value_columns, tsdb_bgn_date=tsdb_bgn_date
            )
