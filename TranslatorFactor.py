import os
from TSDBTranslator2.translator import CMajorContractManager, CTSDBReader
from TSDBTranslator2.translator_from_sql import translate_sqldb_to_tsdb


def translate_signal_from_sql_to_tsdb(
        factor_lbl: str, factor_sql_db_dir: str, tsdb_table_name: str,
        run_mode: str, bgn_date: str, end_date: str, stp_date: str,
        concerned_universe: list[str], by_instru_dir: str,
        tsdb_private_path: str, tsdb_public_path: str,
        factor_value_columns: list[str],  # ["instrument", "value"],
        tsdb_bgn_date: str,
):
    # set factor
    src_sql_db_path = os.path.join(factor_sql_db_dir, factor_lbl + ".db")
    values_rename_mapper = {"value": factor_lbl}

    print("=" * 120)
    print("factor                        = {:>80s}".format(factor_lbl))
    print("factor-SQL      database path = {:>80s}".format(src_sql_db_path))
    print("TSDB-private    database path = {:>80s}".format(tsdb_private_path))
    print("TSDB-public     database path = {:>80s}".format(tsdb_public_path))
    print("begin date                    = {:>80s}".format(bgn_date))
    print("end   date                    = {:>80s}".format(end_date))
    print("stop  date                    = {:>80s}".format(stp_date))
    print("-" * 120)

    # --- load major data
    major_data_manager = CMajorContractManager(universe=concerned_universe, by_instru_dir=by_instru_dir)

    # --- load futures market data tsdb
    futures_md_tsdb_reader = CTSDBReader(tsdb_public_path)

    translate_sqldb_to_tsdb(
        sql_db_path=src_sql_db_path, sql_table_name=factor_lbl,
        values_to_be_translate=factor_value_columns,
        values_rename_mapper=values_rename_mapper,
        mode=run_mode,
        bgn_date=bgn_date, end_date=end_date,
        tsdb_dst_path=tsdb_private_path,
        tsdb_table_name=tsdb_table_name,
        tsdb_bgn_date=tsdb_bgn_date,
        ticker_from_instrument=True,
        instrument_id="instrument",
        contract_id=None,
        major_data_manager=major_data_manager,
        futures_md_tsdb_reader=futures_md_tsdb_reader
    )

    print("-" * 120 + "\n")
    return 0
