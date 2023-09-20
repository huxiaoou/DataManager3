import pandas as pd
from skyrim.whiterun import fix_contract_id, parse_instrument_from_contract_wind
from ManagerDailyIncrementData import CManagerDailyIncrementData


# --------------- with data engine = WDS
class CManagerDailyIncrementDataWithEngineWDS(CManagerDailyIncrementData):
    from DownloadEngineWDS import CDownloadEngineWDS

    def __init__(self, download_engine_wds: CDownloadEngineWDS, **kwargs):
        self.download_engine_wds = download_engine_wds
        super().__init__(**kwargs)

    def _set_save_format(self) -> tuple[bool, str, str]:
        return False, "", "%.4f"


# --- Md from WDS
class CManagerDailyIncrementDataMdWDS(CManagerDailyIncrementDataWithEngineWDS):
    def __init__(self, download_values: list[str], rename_mapper: dict[str, str],
                 file_name_format: str = "md_wds.{}.csv.gz", **kwargs):
        self.download_values = download_values
        self.rename_mapper = rename_mapper
        super().__init__(file_name_format=file_name_format, **kwargs)

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_wds.download_futures_md_by_date(trade_date, self.download_values)
        return df

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raw_df["cid"], raw_df["exchange"] = zip(*raw_df["S_INFO_WINDCODE"].map(lambda z: z.split(".")))
        raw_df["instrument"] = raw_df["cid"].map(parse_instrument_from_contract_wind)
        raw_df["instrument_id_len"] = raw_df["instrument"].map(lambda z: len(z))
        raw_df["loc_id"] = raw_df.apply(
            lambda z: fix_contract_id(z["cid"], z["exchange"], z["instrument_id_len"], trade_date), axis=1)
        filter_exchange = raw_df["exchange"].map(lambda z: z in ["SHF", "INE", "DCE", "CZC", "GFE", "CFE"])
        # Make sure the column orders are the same as those in md_structure.json
        raw_df = raw_df.loc[filter_exchange, ["loc_id", "instrument", "exchange"] + self.download_values]
        raw_df["loc_id"] = raw_df[["loc_id", "exchange"]].apply(lambda z: ".".join(z), axis=1)
        raw_df.rename(mapper=self.rename_mapper, axis=1, inplace=True)
        return raw_df


# --- Position from WDS
class CManagerDailyIncrementDataPositionWDS(CManagerDailyIncrementDataWithEngineWDS):
    def __init__(self, download_values: list[str], rename_mapper: dict[str, str],
                 file_name_format: str = "positions.{}.{{}}.csv.gz", futures_type: str = "C", **kwargs):
        self.download_values = download_values
        self.rename_mapper = rename_mapper
        self.futures_type = futures_type.upper()
        super().__init__(file_name_format=file_name_format.format(self.futures_type), **kwargs)

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_wds.download_futures_positions_by_date(trade_date, self.download_values, self.futures_type)
        return df

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raw_df["cid"], raw_df["exchange"] = zip(*raw_df["S_INFO_WINDCODE"].map(lambda z: z.split(".")))
        raw_df["instrument"] = raw_df["cid"].map(parse_instrument_from_contract_wind)
        raw_df["instrument_id_len"] = raw_df["instrument"].map(lambda z: len(z))
        raw_df["loc_id"] = raw_df.apply(lambda z: fix_contract_id(z["cid"], z["exchange"], z["instrument_id_len"], trade_date), axis=1)
        if self.futures_type == "C":
            filter_exchange = raw_df["exchange"].map(lambda z: z in ["SHF", "INE", "DCE", "CZC", "GFE"])
        else:
            filter_exchange = raw_df["exchange"].map(lambda z: z in ["CFE"])
        # Make sure the column orders are the same as those in fundamental_structure.json
        raw_df = raw_df.loc[filter_exchange, ["loc_id"] + self.download_values + ["instrument", "exchange"]]
        raw_df["loc_id"] = raw_df[["loc_id", "exchange"]].apply(lambda z: ".".join(z), axis=1)
        raw_df.rename(mapper=self.rename_mapper, axis=1, inplace=True)
        return raw_df


# --- Stock from WDS
class CManagerDailyIncrementDataStockWDS(CManagerDailyIncrementDataWithEngineWDS):
    def __init__(self, download_values: list[str], rename_mapper: dict[str, str],
                 file_name_format: str = "stock.{}.csv.gz",
                 header_df: pd.DataFrame = None, id_mapper: dict[str, str] = None,
                 drop_cols: list[str] = None, **kwargs):
        # like the argument "instruments" in download_engine_wapi to download basis
        # header df would confine the final output to "instruments"
        self.download_values = download_values
        self.rename_mapper = rename_mapper
        self.header_df = header_df  # instruments to be downloaded from WDS
        self.id_mapper = id_mapper
        self.drop_cols = drop_cols
        super().__init__(file_name_format=file_name_format, **kwargs)

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_wds.download_futures_stock_by_date(trade_date, self.download_values)
        return df

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        def _parse_wind_code(iid: str):
            instrument, exchange = iid.split(".")
            if exchange not in ["CZC"]:
                instrument = instrument.lower()
            return instrument, exchange

        raw_df["wind_code"] = raw_df["FS_INFO_SCNAME"].map(lambda z: self.id_mapper[z])
        raw_df = pd.merge(left=self.header_df, right=raw_df, on="wind_code", how="left")
        raw_df["instrument"], raw_df["exchange"] = zip(*raw_df["wind_code"].map(_parse_wind_code))
        raw_df = raw_df.set_index("wind_code")
        filter_exchange = raw_df["exchange"].map(lambda z: z in ["SHF", "INE", "DCE", "CZC", "GFE"])  # exclude instruments of CFE
        raw_df = raw_df.loc[filter_exchange, ["instrument", "exchange"] + self.download_values]
        raw_df.drop(axis=1, labels=self.drop_cols, inplace=True)  # if drop_cols = [], nothing would be applied
        raw_df.rename(mapper=self.rename_mapper, axis=1, inplace=True)
        raw_df.reset_index(inplace=True)
        return raw_df
