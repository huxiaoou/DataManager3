import pandas as pd
from skyrim.whiterun import CCalendar
from skyrim.falkreath import CManagerLibReader


def check_tsdb_and_wds(check_values: list[str], bgn_date: str, stp_date: str,
                       futures_dir: str, futures_md_tsdb_db_name: str, futures_md_wds_db_name: str,
                       calendar: CCalendar,
                       verbose: bool):
    error_threshold = {
        "open": 0.003,
        "high": 0.003,
        "low": 0.003,
        "close": 0.003,
        "settle": 0.003,
        "presettle": 0.003,
        "volume": 0.001,
        "oi": 0.001,
        "amount": 0.0001,
    }

    pd.set_option("display.max_rows", 800)
    pd.set_option("display.float_format", "{:.6f}".format)

    print("-" * 120)
    print("futures dir           : {:>90s}".format(futures_dir))
    print("md wds  db name dir   : {:>90s}".format(futures_md_wds_db_name))
    print("md tsdb db name dir   : {:>90s}".format(futures_md_tsdb_db_name))
    print("bgn date              : {:>90s}".format(bgn_date))
    print("stp date              : {:>90s}".format(stp_date))
    print("check values          : {:>90s}".format(",".join(check_values)))
    print("-" * 120 + "\n")

    db_reader_wds = CManagerLibReader(futures_dir, futures_md_wds_db_name)
    db_reader_tsdb = CManagerLibReader(futures_dir, futures_md_tsdb_db_name)
    db_reader_wds.set_default("MD")
    db_reader_tsdb.set_default("MD")

    lookup_values = ["loc_id"] + check_values + (["volume"] if "volume" not in check_values else [])
    for trade_date in calendar.get_iter_list(bgn_date, stp_date, True):
        df0 = db_reader_wds.read_by_date(t_trade_date=trade_date, t_value_columns=lookup_values)
        df0 = df0.loc[df0["volume"] > 0]

        df1 = db_reader_tsdb.read_by_date(t_trade_date=trade_date, t_value_columns=lookup_values)
        df1 = df1.loc[df1["volume"] > 0]

        comparison_df = pd.merge(left=df0, right=df1, on="loc_id", how="inner", suffixes=("_W", "_T"))
        size_wds, size_tsdb, size_comp = len(df0), len(df1), len(comparison_df)
        if len(comparison_df) == 0:
            print("-" * 120)
            print("Not enough data for comparison @ {}".format(trade_date))
            continue

        err_dict = {}
        error_values_to_print = []
        filter_error = None
        errors_are_found = False
        for var_name in check_values:
            if var_name in ["amount"]:
                diff_abs_srs = (comparison_df[var_name + "_W"] / comparison_df[var_name + "_T"] - 1).abs()
            else:
                diff_abs_srs = (comparison_df[var_name + "_W"] - comparison_df[var_name + "_T"]).abs()

            err_dict[var_name] = diff_abs_srs.sum()
            this_var_err_threshold = error_threshold[var_name]
            if this_var_has_err := (err_dict[var_name] > this_var_err_threshold):
                error_values_to_print.append(var_name + "_W")
                error_values_to_print.append(var_name + "_T")
                var_error_srs = diff_abs_srs >= (this_var_err_threshold / len(diff_abs_srs))
                filter_error = var_error_srs if filter_error is None else (filter_error | var_error_srs)
            errors_are_found = errors_are_found or this_var_has_err

        if errors_are_found:
            error_df = comparison_df.loc[filter_error]
            print("-" * 120)
            print("Warning! Errors are found at: {}".format(trade_date))
            print(pd.Series(err_dict))
            print("Rows of comp  df = {}".format(len(comparison_df)))
            print("Rows of Error df = {}".format(len(error_df)))
            print(error_df[["loc_id"] + error_values_to_print])
        else:
            if verbose:
                print("No errors are found at {}".format(trade_date))
                print("size of WDS  {:>6d}".format(size_wds))
                print("size of TSDB {:>6d}".format(size_tsdb))
                print("size of COMP {:>6d}".format(size_comp))
