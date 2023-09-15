import os
import pandas as pd
from DownloadEngineWDS import CDownloadEngineWDS
from skyrim.whiterun import CCalendar
from skyrim.winterhold import check_and_remove_tree, check_and_mkdir


class CManagerDailyIncrementData(object):
    def __init__(self, data_save_dir: str, calendar: CCalendar):
        self.file_name_format = ""
        self.data_save_dir = data_save_dir
        self.calendar = calendar
        self._set_file_name_format()

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
            return os.path.exists(prev_path)

    def __save(self, update_df: pd.DataFrame, trade_date: str,
               using_index: bool, index_label: str, float_format: str
               ):
        save_path = self.__get_date_path(trade_date)
        update_df.to_csv(save_path, index=using_index, index_label=index_label, float_format=float_format)
        return 0

    def _set_file_name_format(self):
        pass

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        pass

    def _set_save_format(self) -> tuple[bool, str, str]:
        pass

    def main(self, run_mode: str, bgn_date: str, stp_date: str):
        if run_mode in ["O"]:
            user_confirmation = input(f"Are you sure to remove {self.data_save_dir}, enter 'y' to continue, else to abandon this operation.")
            if user_confirmation == 'y':
                check_and_remove_tree(self.data_save_dir)
                check_and_mkdir(self.data_save_dir)
            else:
                return 0
        if self.__check_continuity(run_mode, bgn_date):
            using_index, index_label, float_format = self._set_save_format()
            for trade_date in self.calendar.get_iter_list(bgn_date, stp_date, True):
                update_df = self._get_update_data(trade_date)
                self.__save(update_df, trade_date, using_index, index_label, float_format)
        return 0


class CManagerDailyIncrementDataWithEngineWDS(CManagerDailyIncrementData):
    def __init__(self, download_engine_wds: CDownloadEngineWDS, **kwargs):
        self.download_engine_wds = download_engine_wds
        super().__init__(**kwargs)


class CManagerDailyIncrementDataMDWDS(CManagerDailyIncrementDataWithEngineWDS):
    def __init__(self, download_values: list[str], **kwargs):
        self.download_values = download_values
        super().__init__(**kwargs)

    def _set_file_name_format(self):
        self.file_name_format = "md_wds_{}.csv.gz"

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        df = self.download_engine_wds.download_futures_md_by_date(trade_date, self.download_values)
        return df
