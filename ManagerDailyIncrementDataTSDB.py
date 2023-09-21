import pandas as pd
from TSDBTranslator2.translator import CTSDBReader
from ManagerDailyIncrementData import CManagerDailyIncrementData
from skyrim.whiterun import CInstrumentInfoTable, fix_contract_id, parse_instrument_from_contract_wind


# --------------- with data engine = TSDB
# separate from ManagerDailyIncrementData, dut to TSDB and pyqts package are introduced
# these modules are only available on GH-Server, and not available on local windows platform

class CManagerDailyIncrementDataWithEngineTSDB(CManagerDailyIncrementData):
    def __init__(self, download_engine_tsdb: CTSDBReader, instru_info_table: CInstrumentInfoTable = None, **kwargs):
        self.download_engine_tsdb = download_engine_tsdb
        self.instru_info_table = instru_info_table
        super().__init__(**kwargs)

    def _set_save_format(self) -> tuple[bool, str, str]:
        return False, "", "%.4f"

    def _instrument_to_exchange(self, instrument: str) -> str:
        exchange_remapper = {
            "CFFEX": "CFE",
            "SHFE": "SHF",
            "INE": "INE",
            "DCE": "DCE",
            "CZCE": "CZC",
            "GFE": "GFE",
            "NAN": "NAN",
        }
        try:
            exch = self.instru_info_table.get_exchangeId(instrument)
        except KeyError:
            print(instrument, "abandoned")
            exch = "NAN"
        return exchange_remapper[exch]


# --- Md from TSDB
class CManagerDailyIncrementDataMdTSDB(CManagerDailyIncrementDataWithEngineTSDB):
    def __init__(self, download_values: list[str], file_name_format: str = "md_tsdb.{}.csv.gz", patch_data_file: str = "patch_data_tsdb.csv", **kwargs):
        self.download_values = download_values
        patch_df = pd.read_csv(patch_data_file, dtype=str)
        self.manager_patch = {k: v for k, v in patch_df.groupby(by="trade_date")}
        super().__init__(file_name_format=file_name_format, **kwargs)

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_tsdb.read_by_date(self.download_values, trade_date)
        return df

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raw_df["amount"] = raw_df["amount"] / 1e4  # set unit = WAN YUAN
        raw_df["cid"] = raw_df["ticker"].map(lambda z: z.decode())
        raw_df["instrument"] = raw_df["cid"].map(parse_instrument_from_contract_wind)
        raw_df["instrument_id_len"] = raw_df["instrument"].map(lambda z: len(z))
        raw_df["exchange"] = raw_df["instrument"].map(self._instrument_to_exchange)
        raw_df["cid"] = raw_df["cid"].map(lambda z: z.upper())
        raw_df["instrument"] = raw_df["instrument"].map(lambda z: z.upper())
        raw_df["loc_id"] = raw_df.apply(
            lambda z: fix_contract_id(z["cid"], z["exchange"], z["instrument_id_len"], trade_date), axis=1)
        raw_df["wind_code"] = raw_df[["cid", "exchange"]].apply(lambda z: z["cid"] + "." + z["exchange"], axis=1)
        filter_exchange = raw_df["exchange"].map(lambda z: z in ["SHF", "INE", "DCE", "CZC", "GFE", "CFE"])
        # Make sure the column orders are the same as those in md_structure.json
        raw_df = raw_df.loc[filter_exchange, ["loc_id", "instrument", "exchange", "wind_code"] + self.download_values]
        raw_df["loc_id"] = raw_df[["loc_id", "exchange"]].apply(lambda z: ".".join(z), axis=1)
        raw_df.sort_values("loc_id", ascending=True, inplace=True)
        return raw_df

    def patch(self, bgn_date: str, stp_date: str):
        for trade_date in self.calendar.get_iter_list(bgn_date, stp_date, True):
            if (patch_df := self.manager_patch.get(trade_date, None)) is not None:
                src_df = self._get_date_df(trade_date).set_index("loc_id")
                src_df.update(patch_df.set_index("loc_id"))
                src_df.reset_index(inplace=True)
                self._save_to_file(src_df, trade_date)
        return 0


# --- M01 from TSDB
class CManagerDailyIncrementDataM01TSDB(CManagerDailyIncrementDataWithEngineTSDB):
    def __init__(self, download_values: list[str], file_name_format: str = None, **kwargs):
        self.download_values = download_values
        super().__init__(file_name_format=file_name_format, **kwargs)

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_tsdb.read_by_date(self.download_values, trade_date, freq="m01e")
        return df

    @staticmethod
    def _get_filter_exch(df: pd.DataFrame):
        pass

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        # use instrument and contract info to drop unnecessary rows
        raw_df.dropna(axis=0, how="all", inplace=True, subset=self.download_values)
        raw_df["cid"] = raw_df["ticker"].map(lambda z: z.decode())  # example of cid:"rb2305"
        raw_df["instrument"] = raw_df["cid"].map(parse_instrument_from_contract_wind)  # example of instrument: "rb"
        raw_df["exchange"] = raw_df["instrument"].map(self._instrument_to_exchange)
        filter_exch_or_instru = self._get_filter_exch(raw_df)
        raw_df = raw_df.loc[filter_exch_or_instru].copy()

        # reformat cid, instrument, exchange
        raw_df["instrument_id_len"] = raw_df["instrument"].map(lambda z: len(z))
        raw_df["cid"] = raw_df["cid"].map(lambda z: z.upper())
        raw_df["instrument"] = raw_df["instrument"].map(lambda z: z.upper())
        raw_df["loc_id"] = raw_df.apply(
            lambda z: fix_contract_id(z["cid"], z["exchange"], z["instrument_id_len"], trade_date), axis=1)
        raw_df["wind_code"] = raw_df[["cid", "exchange"]].apply(lambda z: ".".join(z), axis=1)
        raw_df["loc_id"] = raw_df[["loc_id", "exchange"]].apply(lambda z: ".".join(z), axis=1)

        # add extra information
        raw_df["timestamp"] = raw_df["tp"].map(lambda z: int(z / 1e9) - 60)
        raw_df["amount"] = raw_df["amount"] / 1e4  # set unit = WAN YUAN

        # Make sure the column orders are the same as those in md_structure.json
        raw_df = raw_df[["timestamp", "loc_id", "instrument", "exchange", "wind_code"] + self.download_values]
        raw_df.sort_values(by=["timestamp", "loc_id"], ascending=True, inplace=True)
        return raw_df


class CManagerDailyIncrementDataEM01TSDB(CManagerDailyIncrementDataM01TSDB):
    def __init__(self, file_name_format: str = "em01_tsdb.{}.csv.gz", **kwargs):
        super().__init__(file_name_format=file_name_format, **kwargs)

    @staticmethod
    def _get_filter_exch(df: pd.DataFrame):
        return df["exchange"].map(lambda z: z in ["CFE"])


class CManagerDailyIncrementDataCM01TSDB(CManagerDailyIncrementDataM01TSDB):
    def __init__(self, file_name_format: str = "cm01_tsdb.{}.csv.gz", **kwargs):
        super().__init__(file_name_format=file_name_format, **kwargs)

    @staticmethod
    def _get_filter_exch(df: pd.DataFrame):
        return df["exchange"].map(lambda z: z in ["SHF", "INE", "DCE", "CZC", "GFE"])
