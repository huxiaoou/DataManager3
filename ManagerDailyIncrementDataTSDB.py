import pandas as pd
from TSDBTranslator2.translator import CTSDBReader
from ManagerDailyIncrementData import CManagerDailyIncrementData
from skyrim.whiterun import CInstrumentInfoTable, fix_contract_id, parse_instrument_from_contract_wind


# --------------- with data engine = TSDB
# separate from ManagerDailyIncrementData, dut to TSDB and pyqts package are introduced
# these modules are only available on GH-Server, and not available on local windows platform

class CManagerDailyIncrementDataWithEngineTSDB(CManagerDailyIncrementData):
    def __init__(self, download_engine_tsdb: CTSDBReader, **kwargs):
        self.download_engine_tsdb = download_engine_tsdb
        super().__init__(**kwargs)


# --- Md from TSDB
class CManagerDailyIncrementDataMdTSDB(CManagerDailyIncrementDataWithEngineTSDB):
    def __init__(self, download_values: list[str], file_name_format: str = "md_tsdb.{}.csv.gz", instru_info_table: CInstrumentInfoTable = None, **kwargs):
        self.download_values = download_values
        self.instru_info_table = instru_info_table
        super().__init__(**kwargs)
        self.file_name_format = file_name_format

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_tsdb.read_by_date(self.download_values, trade_date)
        return df

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        exchange_remapper = {
            "CFFEX": "CFE",
            "SHFE": "SHF",
            "INE": "INE",
            "DCE": "DCE",
            "CZCE": "CZC",
            "GFE": "GFE",
        }
        raw_df["amount"] = raw_df["amount"] / 1e4  # set unit = WAN YUAN
        raw_df["cid"] = raw_df["ticker"].map(lambda z: z.decode())
        raw_df["instrument"] = raw_df["cid"].map(parse_instrument_from_contract_wind)
        raw_df["instrument_id_len"] = raw_df["instrument"].map(lambda z: len(z))
        raw_df["exchange"] = raw_df["instrument"].map(lambda z: exchange_remapper[self.instru_info_table.get_exchangeId(z)])
        raw_df["cid"] = raw_df["cid"].map(lambda z: z.upper())
        raw_df["instrument"] = raw_df["instrument"].map(lambda z: z.upper())
        raw_df["loc_id"] = raw_df.apply(
            lambda z: fix_contract_id(z["cid"], z["exchange"], z["instrument_id_len"], trade_date), axis=1)
        raw_df["wind_code"] = raw_df[["cid", "exchange"]].apply(lambda z: z["cid"] + "." + z["exchange"], axis=1)
        filter_exchange = raw_df["exchange"].map(lambda z: z in ["DCE", "SHF", "CZC", "INE", "GFE", "CFE"])
        # Make sure the column orders are the same as those in md_structure.json
        raw_df = raw_df.loc[filter_exchange, ["loc_id", "instrument", "exchange", "wind_code"] + self.download_values]
        raw_df["loc_id"] = raw_df[["loc_id", "exchange"]].apply(lambda z: ".".join(z), axis=1)
        return raw_df


# --- M01 from TSDB
class CManagerDailyIncrementDataM01TSDB(CManagerDailyIncrementDataWithEngineTSDB):
    def __init__(self, download_values: list[str], file_name_format: str = "em01_tsdb.{}.csv.gz", instru_info_table: CInstrumentInfoTable = None, **kwargs):
        self.download_values = download_values
        self.instru_info_table = instru_info_table
        super().__init__(**kwargs)
        self.file_name_format = file_name_format

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_tsdb.read_by_date(self.download_values, trade_date, freq="m01e")
        return df

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        exchange_remapper = {
            "CFFEX": "CFE",
            "SHFE": "SHF",
            "INE": "INE",
            "DCE": "DCE",
            "CZCE": "CZC",
            "GFE": "GFE",
        }

        # use instrument and contract info to drop unnecessary rows
        raw_df.dropna(axis=0, how="all", inplace=True, subset=self.download_values)
        raw_df["cid"] = raw_df["ticker"].map(lambda z: z.decode())  # example of cid:"rb2305"
        raw_df["instrument"] = raw_df["cid"].map(parse_instrument_from_contract_wind)  # example of instrument: "rb"
        filter_exch_or_instru = raw_df["instrument"].map(lambda z: z in ["IH", "IF", "IC", "IM"])
        raw_df = raw_df.loc[filter_exch_or_instru].copy()

        # reformat cid, instrument, exchange
        raw_df["instrument_id_len"] = raw_df["instrument"].map(lambda z: len(z))
        raw_df["exchange"] = raw_df["instrument"].map(lambda z: exchange_remapper[self.instru_info_table.get_exchangeId(z)])
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
        return raw_df