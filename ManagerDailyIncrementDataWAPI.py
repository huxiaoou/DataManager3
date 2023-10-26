import pandas as pd
from DownloadEngineWAPI import CDownloadEngineWAPI
from ManagerDailyIncrementData import CManagerDailyIncrementData


# --------------- with data engine = WAPI
class CManagerDailyIncrementDataWithEngineWAPI(CManagerDailyIncrementData):

    def __init__(self, download_engine_wapi: CDownloadEngineWAPI, **kwargs):
        # For basis of futures ONLY now - 20230918
        # data engine api includes instruments to be downloaded
        # this would confine the final output to the argument "instruments"
        self.download_engine_wapi = download_engine_wapi
        super().__init__(**kwargs)

    def _set_save_format(self) -> tuple[bool, str, str]:
        return True, "wind_code", "%.8f"


# --- Basis from WAPI
class CManagerDailyIncrementDataBasisWAPI(CManagerDailyIncrementDataWithEngineWAPI):
    def __init__(self, download_values: list[str], rename_mapper: dict[str, str], exchange_filter: list[str],
                 file_name_format: str, using_full_id: bool = True, **kwargs):
        self.download_values = download_values
        self.rename_mapper = rename_mapper
        self.using_full_id = using_full_id
        self.exchange_filter = exchange_filter  # commodity = ["SHF", "INE", "DCE", "CZC", "GFE"], financial = ["CFE"]
        super().__init__(file_name_format=file_name_format, **kwargs)

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_wapi.download_by_date_api(trade_date, self.download_values, using_full_id=self.using_full_id)
        return df

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        raw_df = self.download_engine_wapi.parse_wind_code(raw_df)
        filter_exchange = raw_df["exchange"].map(lambda z: z in self.exchange_filter)  # exclude instruments of CFE
        raw_df = raw_df.loc[filter_exchange].copy()
        raw_df.rename(mapper=self.rename_mapper, axis=1, inplace=True)
        return raw_df
