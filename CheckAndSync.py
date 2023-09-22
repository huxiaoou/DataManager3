import os
import scp
import paramiko
import pandas as pd
from skyrim.whiterun import CCalendar, SetFontGreen, SetFontYellow, SetFontRed


def check_tsdb_and_wds(check_values: list[str], bgn_date: str, stp_date: str,
                       futures_by_date_dir: str, futures_md_tsdb_db_name: str, futures_md_wds_db_name: str,
                       calendar: CCalendar, verbose: bool, display_max_rows: int = 800, display_float_format: str = "{:.6f}"):
    pd.set_option("display.max_rows", display_max_rows)
    pd.set_option("display.float_format", display_float_format.format)

    error_thresholds = {
        "open": 0.003,
        "high": 0.003,
        "low": 0.003,
        "close": 0.003,
        "settle": 0.003,
        "presettle": 0.003,
        "volume": 0.001,
        "oi": 0.001,
        "amount": 0.005,
    }

    print("-" * 120)
    print("futures by date dir   : {:>90s}".format(futures_by_date_dir))
    print("md wds  db name dir   : {:>90s}".format(futures_md_wds_db_name))
    print("md tsdb db name dir   : {:>90s}".format(futures_md_tsdb_db_name))
    print("bgn date              : {:>90s}".format(bgn_date))
    print("stp date              : {:>90s}".format(stp_date))
    print("check values          : {:>90s}".format(",".join(check_values)))
    print("-" * 120 + "\n")

    lookup_values = check_values + (["volume"] if "volume" not in check_values else [])
    for trade_date in calendar.get_iter_list(bgn_date, stp_date, True):
        path0 = os.path.join(futures_by_date_dir, trade_date[0:4], trade_date, f"md_wds.{trade_date}.csv.gz")
        path1 = os.path.join(futures_by_date_dir, trade_date[0:4], trade_date, f"md_tsdb.{trade_date}.csv.gz")
        df0 = pd.read_csv(path0)
        df0 = df0.loc[df0["volume"] > 0, ["loc_id"] + lookup_values]
        df1 = pd.read_csv(path1).rename(mapper={"vol": "volume"}, axis=1)
        df1 = df1.loc[df1["volume"] > 0, ["loc_id"] + lookup_values]

        comparison_df = pd.merge(left=df0, right=df1, on="loc_id", how="inner", suffixes=("_W", "_T"))
        size_wds, size_tsdb, size_comp = len(df0), len(df1), len(comparison_df)
        if len(comparison_df) == 0:
            print("-" * 120)
            print("Not enough data for comparison @ {}".format(trade_date))
            continue

        err_dict = []
        error_values_to_print = []
        filter_error = None
        errors_are_found = False
        for var_name in check_values:
            if var_name in ["volume", "amount"]:
                diff_abs_srs = (comparison_df[var_name + "_W"] / comparison_df[var_name + "_T"] - 1).abs()
            else:
                diff_abs_srs = (comparison_df[var_name + "_W"] - comparison_df[var_name + "_T"]).abs()

            err_abs_sum = diff_abs_srs.sum()
            err_threshold = error_thresholds[var_name]
            if this_var_has_err := (err_abs_sum > err_threshold):
                error_values_to_print.append(var_name + "_W")
                error_values_to_print.append(var_name + "_T")
                var_error_srs = diff_abs_srs >= (err_threshold / len(diff_abs_srs))
                filter_error = var_error_srs if filter_error is None else (filter_error | var_error_srs)
            errors_are_found = errors_are_found or this_var_has_err
            err_dict.append({"var": var_name, "err": err_abs_sum, "threshold": err_threshold})

        if errors_are_found:
            error_df = comparison_df.loc[filter_error]
            print("-" * 120)
            print(f"Warning! Errors are found at: {SetFontRed(trade_date)}")
            print(pd.DataFrame(err_dict))
            print(f"Rows of comp  df = {SetFontYellow(f'{len(comparison_df)}')}")
            print(f"Rows of Error df = {SetFontYellow(f'{len(error_df)}')}")
            print(error_df[["loc_id"] + error_values_to_print])
            print("-" * 12 + "\n")
        else:
            if verbose:
                print(f"No errors are found at {SetFontGreen(trade_date)}, "
                      f"size of  WDS = {SetFontYellow(f'{size_wds:>4d}')}"
                      f"        TSDB = {SetFontYellow(f'{size_tsdb:>4d}')}"
                      f"  COMPARISON = {SetFontYellow(f'{size_comp:>4d}')}")
    return 0


class CAgentSync(object):
    def __init__(self, hostname: str, username: str, port: int = 22):
        self.ssh = paramiko.SSHClient()
        self.ssh.load_system_host_keys()
        self.ssh.connect(hostname=hostname, username=username, port=port)
        self.scp = scp.SCPClient(self.ssh.get_transport())

    def copy_from_remote_to_local(self, remote_path: str, local_path: str, verbose: bool):
        try:
            self.scp.get(remote_path, local_path)
            if verbose:
                print(f"... copy {SetFontYellow(remote_path)} to {SetFontGreen(local_path)}")
        except scp.SCPException:
            print(f"... {SetFontRed('FAILED')} to download {SetFontYellow(remote_path)}")
        return 0

    def copy_from_local_to_remote(self, local_path: str, remote_path: str, verbose: bool):
        try:
            self.scp.put(local_path, remote_path)
            if verbose:
                print(f"... copy {SetFontYellow(local_path)} to {SetFontGreen(remote_path)}")
        except scp.SCPException:
            print(f"... {SetFontRed('FAILED')} to upload {SetFontYellow(local_path)}")
        return 0

    def close(self):
        self.scp.close()
        self.ssh.close()
        return 0


class CAgentSyncDailyIncrement(CAgentSync):
    def __init__(self, remote_root_dir: str, local_root_dir: str,
                 remote_join_sign: str = "/", local_join_sign: str = "\\",
                 **kwargs):
        self.remote_root_dir = remote_root_dir
        self.local_root_dir = local_root_dir
        self.remote_join_sign = remote_join_sign
        self.local_join_sign = local_join_sign
        super().__init__(**kwargs)

    def __get_remote_path(self, trade_date: str, file_name: str):
        _path = self.remote_join_sign.join([self.remote_root_dir, trade_date[0:4], trade_date, file_name])
        return _path

    def __get_local_path(self, trade_date: str, file_name: str):
        _path = self.local_join_sign.join([self.local_root_dir, trade_date[0:4], trade_date, file_name])
        return _path

    def download_by_date(self, file_name_formats: list[str], trade_date: str, verbose: bool):
        for file_name_format in file_name_formats:
            target_file = file_name_format.format(trade_date)
            remote_path = self.__get_remote_path(trade_date, target_file)
            local_path = self.__get_local_path(trade_date, target_file)
            self.copy_from_remote_to_local(remote_path=remote_path, local_path=local_path, verbose=verbose)
        return 0

    def upload_by_date(self, file_name_formats: list[str], trade_date: str, verbose: bool):
        for file_name_format in file_name_formats:
            target_file = file_name_format.format(trade_date)
            remote_path = self.__get_remote_path(trade_date, target_file)
            local_path = self.__get_local_path(trade_date, target_file)
            self.copy_from_local_to_remote(local_path=local_path, remote_path=remote_path, verbose=verbose)
        return 0

    def download_by_dates(self, file_name_formats: list[str], bgn_date: str, stp_date: str, calendar: CCalendar, verbose: bool):
        for trade_date in calendar.get_iter_list(bgn_date, stp_date, True):
            self.download_by_date(file_name_formats, trade_date, verbose)
        return 0

    def upload_by_dates(self, file_name_formats: list[str], bgn_date: str, stp_date: str, calendar: CCalendar, verbose: bool):
        for trade_date in calendar.get_iter_list(bgn_date, stp_date, True):
            self.upload_by_date(file_name_formats, trade_date, verbose)
        return 0
