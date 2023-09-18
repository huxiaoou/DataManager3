import os
import pandas as pd
from DownloadEngineWDS import CDownloadEngineWDS
from DownloadEngineWAPI import CDownloadEngineWAPI
from TSDBTranslator2.translator import CTSDBReader
from skyrim.whiterun import CCalendar, CInstrumentInfoTable, parse_instrument_from_contract_wind, fix_contract_id
from skyrim.winterhold import check_and_remove_tree, check_and_mkdir
from skyrim.falkreath import CManagerLibWriterByDate, CLib1Tab1


class CManagerDailyIncrementData(object):
    def __init__(self, data_save_dir: str, calendar: CCalendar):
        self.file_name_format = "NotInit"
        self.data_save_dir = data_save_dir
        self.calendar = calendar

    def __get_date_file(self, trade_date: str):
        return self.file_name_format.format(trade_date)

    def __get_date_path(self, trade_date: str):
        trade_date_file = self.__get_date_file(trade_date)
        return os.path.join(self.data_save_dir, trade_date[0:4], trade_date, trade_date_file)

    def __check_continuity(self, run_mode: str, append_date: str) -> bool:
        if run_mode in ["O"]:
            return True
        else:
            prev_date = self.calendar.get_next_date(append_date, -1)
            prev_path = self.__get_date_path(prev_date)
            this_path = self.__get_date_path(append_date)
            if not os.path.exists(prev_path):
                print(f"... Error! Prev path does not exist. Prev path = {prev_path}")
                return False
            elif os.path.exists(this_path):
                print(f"... Error! This path already  exist. This path = {this_path}")
                return False
            else:
                return True

    def __save(self, update_df: pd.DataFrame, trade_date: str,
               using_index: bool, index_label: str, float_format: str
               ):
        save_path = self.__get_date_path(trade_date)
        update_df.to_csv(save_path, index=using_index, index_label=index_label, float_format=float_format)
        return 0

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        pass

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        pass

    def _set_save_format(self) -> tuple[bool, str, str]:
        pass

    def main(self, run_mode: str, bgn_date: str, stp_date: str):
        if run_mode in ["O"]:
            user_confirmation = input(f"Are you sure to remove {self.data_save_dir}, enter 'y' to continue, else to abandon this operation. [y/n]")
            if user_confirmation == 'y':
                check_and_remove_tree(self.data_save_dir)
                check_and_mkdir(self.data_save_dir)
            else:
                return 0
        using_index, index_label, float_format = self._set_save_format()
        for trade_date in self.calendar.get_iter_list(bgn_date, stp_date, True):
            if self.__check_continuity(run_mode, append_date=bgn_date):
                update_df = self._get_update_data(trade_date)
                update_df = self._reformat(update_df, trade_date)
                self.__save(update_df, trade_date, using_index, index_label, float_format)
        return 0

    def __check_db_continuity(self, dst_db_writer: CManagerLibWriterByDate, run_mode: str, append_date: str) -> int:
        if run_mode in ["O"]:
            return 0
        else:
            return dst_db_writer.check_continuity(append_date, self.calendar)

    def to_sqlite_database(self, dst_db_save_dir: str, dst_db_name: str, dst_db_struct: CLib1Tab1,
                           run_mode: str, bgn_date: str, stp_date: str):
        dst_db_writer = CManagerLibWriterByDate(dst_db_save_dir, dst_db_name)
        dst_db_writer.initialize_table(dst_db_struct.m_tab, run_mode in ["O"])
        if self.__check_db_continuity(dst_db_writer, run_mode, bgn_date) == 0:
            for trade_date in self.calendar.get_iter_list(bgn_date, stp_date, True):
                df = pd.read_csv(self.__get_date_path(trade_date))
                dst_db_writer.update_by_date(trade_date, df, t_using_index=False)
        dst_db_writer.close()
        return 0


# --------------- with data engine = WDS
class CManagerDailyIncrementDataWithEngineWDS(CManagerDailyIncrementData):
    def __init__(self, download_engine_wds: CDownloadEngineWDS, **kwargs):
        self.download_engine_wds = download_engine_wds
        super().__init__(**kwargs)


# --------------- with data engine = WAPI
class CManagerDailyIncrementDataWithEngineWAPI(CManagerDailyIncrementData):
    def __init__(self, download_engine_wapi: CDownloadEngineWAPI, **kwargs):
        # For basis of futures ONLY now - 20230918
        # data engine api includes instruments to be downloaded
        # this would confine the final output to the argument "instruments"
        self.download_engine_wapi = download_engine_wapi
        super().__init__(**kwargs)


# --------------- with data engine = TSDB
class CManagerDailyIncrementDataWithEngineTSDB(CManagerDailyIncrementData):
    def __init__(self, download_engine_tsdb: CTSDBReader, **kwargs):
        self.download_engine_tsdb = download_engine_tsdb
        super().__init__(**kwargs)


# --- Md from WDS
class CManagerDailyIncrementDataMdWDS(CManagerDailyIncrementDataWithEngineWDS):
    def __init__(self, download_values: list[str], file_name_format: str = "md_wds.{}.csv.gz", **kwargs):
        self.download_values = download_values
        super().__init__(**kwargs)
        self.file_name_format = file_name_format

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
        return raw_df


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


# --- Stock from WDS
class CManagerDailyIncrementDataStockWDS(CManagerDailyIncrementDataWithEngineWDS):
    def __init__(self, download_values: list[str], file_name_format: str = "stock.{}.csv.gz",
                 header_df: pd.DataFrame = None, id_mapper: dict[str, str] = None, drop_cols: list[str] = None, **kwargs):
        # like the argument "instruments" in download_engine_wapi to download basis
        # header df would confine the final output to "instruments"
        self.download_values = download_values
        self.header_df = header_df  # instruments to be downloaded from WDS
        self.id_mapper = id_mapper
        self.drop_cols = drop_cols
        super().__init__(**kwargs)
        self.file_name_format = file_name_format

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
        filter_exchange = raw_df["exchange"].map(lambda z: z in ["SHF", "INE", "DCE", "CZC", "GFE"])
        raw_df = raw_df.loc[filter_exchange, ["instrument", "exchange"] + self.download_values]
        raw_df.drop(axis=1, labels=self.drop_cols, inplace=True)  # if drop_cols = [], nothing would be applied
        return raw_df


# --- Basis from WAPI
class CManagerDailyIncrementDataBasisWAPI(CManagerDailyIncrementDataWithEngineWAPI):
    def __init__(self, download_values: list[str], file_name_format: str = "basis.{}.csv.gz", using_full_id: bool = False, **kwargs):
        self.download_values = download_values
        self.using_full_id = using_full_id
        super().__init__(**kwargs)
        self.file_name_format = file_name_format

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_wapi.download_by_date_api(trade_date, self.download_values, using_full_id=self.using_full_id)
        return df


# --- Position from WDS
class CManagerDailyIncrementDataPositionWDS(CManagerDailyIncrementDataWithEngineWDS):
    def __init__(self, download_values: list[str], file_name_format: str = "positions.{}.{{}}.csv.gz", futures_type: str = "C", **kwargs):
        self.download_values = download_values
        self.futures_type = futures_type.upper()
        super().__init__(**kwargs)
        self.file_name_format = file_name_format.format(self.futures_type)

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
        raw_df = raw_df.loc[filter_exchange, ["TRADE_DT", "loc_id"] + self.download_values + ["instrument", "exchange"]]
        raw_df["loc_id"] = raw_df[["loc_id", "exchange"]].apply(lambda z: ".".join(z), axis=1)
        return raw_df
