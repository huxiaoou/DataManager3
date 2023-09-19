import sys
import pandas as pd
from WindPy import w as wind_api


class CDownloadEngineWAPI(object):
    def __init__(self, instruments: list[str]):
        self.instruments = instruments
        self.instruments_short = [_.split(".")[0] for _ in instruments]
        wind_api.start()

    def download_by_date_api(self, trade_date: str, download_values: list[str], using_full_id: bool = False):
        cmd_str_instruments = ",".join(self.instruments)
        cmd_str_download_values = ",".join(download_values)
        downloaded_data = wind_api.wss(cmd_str_instruments, cmd_str_download_values, f"tradeDate={trade_date}")
        if downloaded_data.ErrorCode != 0:
            print("... Error! When download data from WIND @ {}".format(trade_date))
            print("... ErrorCode = {}".format(downloaded_data.ErrorCode))
            print("... Program will terminate at once.")
            print("... Please check again.")
            sys.exit()
        else:
            col_names = self.instruments if using_full_id else self.instruments_short
            df = pd.DataFrame(downloaded_data.Data, index=download_values, columns=col_names).T
        return df


def parse_wind_code(df: pd.DataFrame):
    def _parse_wind_code(iid: str):
        instrument, exchange = iid.split(".")
        if exchange not in ["CZC"]:
            instrument = instrument.lower()
        return instrument, exchange

    df["instrument"], df["exchange"] = zip(*df.index.map(_parse_wind_code))
    return df
