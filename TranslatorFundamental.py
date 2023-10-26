from TSDBTranslator2.translator import CTSDBReader
from TSDBTranslator2.translator_from_csv import translate_csv_to_tsdb


def translate_fundamental_from_csv_to_tsdb(
        factor_lbl: str,
        src_csv_db_path: str, tsdb_table_name: str,  # ="huxo.fundamental"
        run_mode: str, bgn_date: str, end_date: str, stp_date: str,
        custom_ts_db_path: str, futures_md_ts_db_path: str,
        tsdb_bgn_date: str = "20120101",
):
    src_data_columns_config = {
        "BASIS": ["instrument", "basis", "basis_rate"],
        "BASIS_CFE": ["instrument", "basis", "basis_rate", "basis_rate_annual"],
        "STOCK": ["instrument", "in_stock_total", "in_stock", "available_in_stock"],
    }
    values_rename_mapper = {
        "BASIS": {},
        "BASIS_CFE": {
            "basis": "basis_cfe",
            "basis_rate": "basis_rate_cfe",
            "basis_rate_annual": "basis_rate_annual_cfe", },
        "STOCK": {},
    }[factor_lbl.upper()]

    # set factor
    csv_file_format = f"{factor_lbl}.{{}}.csv.gz"  # => "basis.{}.csv.gz"
    values_to_be_translated = src_data_columns_config[factor_lbl.upper()]

    print("=" * 120)
    print("factor                        = {:>80s}".format(factor_lbl.upper()))
    print("factor-CSV      database path = {:>80s}".format(src_csv_db_path))
    print("factor-TSDB     database path = {:>80s}".format(custom_ts_db_path))
    print("futures-MD-TSDB database path = {:>80s}".format(futures_md_ts_db_path))
    print("begin date                    = {:>80s}".format(bgn_date))
    print("end   date                    = {:>80s}".format(end_date))
    print("stop  date                    = {:>80s}".format(stp_date))
    print("-" * 120)

    # --- load futures market data tsdb
    futures_md_tsdb_reader = CTSDBReader(futures_md_ts_db_path)
    translate_csv_to_tsdb(
        csv_files_root_dir=src_csv_db_path, csv_file_format=csv_file_format,
        values_to_be_translate=values_to_be_translated,
        values_rename_mapper=values_rename_mapper,
        mode=run_mode,
        bgn_date=bgn_date, end_date=end_date,
        tsdb_dst_path=custom_ts_db_path,
        tsdb_table_name=tsdb_table_name,
        tsdb_bgn_date=tsdb_bgn_date,
        ticker_from_instrument=True,
        instrument_id="instrument",
        contract_id=None,
        futures_md_tsdb_reader=futures_md_tsdb_reader
    )

    print("-" * 120 + "\n")
    return 0
