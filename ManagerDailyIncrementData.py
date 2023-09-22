import os
import datetime as dt
import pandas as pd
from skyrim.whiterun import CCalendar, SetFontGreen, SetFontYellow
from skyrim.winterhold2 import check_and_mkdir
from skyrim.falkreath import CManagerLibWriterByDate, CLib1Tab1


class CManagerDailyIncrementData(object):
    def __init__(self, file_name_format: str, data_save_dir: str, calendar: CCalendar):
        self.file_name_format = file_name_format
        self.data_save_dir = data_save_dir
        self.calendar = calendar

    def __get_date_file_name(self, trade_date: str):
        return self.file_name_format.format(trade_date)

    def __check_and_mkdir_for_date_file_dir(self, trade_date: str):
        check_and_mkdir(os.path.join(self.data_save_dir, trade_date[0:4]))
        check_and_mkdir(os.path.join(self.data_save_dir, trade_date[0:4], trade_date))
        return 0

    def __get_date_file_path(self, trade_date: str):
        trade_date_file = self.__get_date_file_name(trade_date)
        return os.path.join(self.data_save_dir, trade_date[0:4], trade_date, trade_date_file)

    def _get_date_df(self, trade_date: str) -> pd.DataFrame:
        df = pd.read_csv(self.__get_date_file_path(trade_date), dtype=str)
        return df

    def _save_to_file(self, df: pd.DataFrame, trade_date: str):
        df.to_csv(self.__get_date_file_path(trade_date), index=False)
        return 0

    def __check_continuity(self, run_mode: str, append_date: str) -> bool:
        if run_mode in ["O"]:
            return True
        else:
            prev_date = self.calendar.get_next_date(append_date, -1)
            prev_path = self.__get_date_file_path(prev_date)
            this_path = self.__get_date_file_path(append_date)
            if not os.path.exists(prev_path):
                print(f"... Error! {SetFontGreen('Prev path does not exist')}. This date = {SetFontYellow(append_date)}, prev_date = {SetFontYellow(prev_date)}, Prev path = {prev_path}")
                return False
            elif os.path.exists(this_path):
                print(f"... Error! {SetFontGreen('This path already  exist')}. This date = {SetFontYellow(append_date)}, prev_date = {SetFontYellow(prev_date)}, This path = {this_path}")
                return False
            else:
                return True

    def __save(self, update_df: pd.DataFrame, trade_date: str,
               using_index: bool, index_label: str, float_format: str
               ):
        save_path = self.__get_date_file_path(trade_date)
        update_df.to_csv(save_path, index=using_index, index_label=index_label, float_format=float_format)
        return 0

    def _get_update_data(self, trade_date: str) -> pd.DataFrame:
        pass

    def _reformat(self, raw_df: pd.DataFrame, trade_date: str) -> pd.DataFrame:
        pass

    def _set_save_format(self) -> tuple[bool, str, str]:
        pass

    def main(self, run_mode: str, bgn_date: str, stp_date: str, verbose: bool = False):
        if run_mode in ["O"]:
            user_confirmation = input(f"Are you sure to {SetFontYellow('REMOVE')} files like '{self.file_name_format}' in {self.data_save_dir},"
                                      f" enter 'y' to continue, else to abandon this operation. {SetFontYellow('[y/n]')}")
            if user_confirmation == 'y':
                for trade_year in os.listdir(self.data_save_dir):
                    for trade_date in os.listdir(os.path.join(self.data_save_dir, trade_year)):
                        if os.path.exists(dst_path := self.__get_date_file_path(trade_date)):
                            os.remove(dst_path)
            else:
                return 0
        using_index, index_label, float_format = self._set_save_format()
        for trade_date in self.calendar.get_iter_list(bgn_date, stp_date, True):
            if self.__check_continuity(run_mode, append_date=trade_date):
                update_df = self._get_update_data(trade_date)
                update_df = self._reformat(update_df, trade_date)
                self.__check_and_mkdir_for_date_file_dir(trade_date)
                self.__save(update_df, trade_date, using_index, index_label, float_format)
                if verbose:
                    print(f"... @ {dt.datetime.now()} {SetFontYellow(self.__get_date_file_name(trade_date))} downloaded")
        return 0

    def __check_db_continuity(self, dst_db_writer: CManagerLibWriterByDate, run_mode: str, append_date: str) -> int:
        if run_mode in ["O"]:
            return 0
        else:
            return dst_db_writer.check_continuity(append_date, self.calendar)

    def to_sqlite_database(self, dst_db_save_dir: str, dst_db_struct: CLib1Tab1,
                           run_mode: str, bgn_date: str, stp_date: str):
        dst_db_writer = CManagerLibWriterByDate(dst_db_save_dir, dst_db_struct.m_lib_name)
        dst_db_writer.initialize_table(dst_db_struct.m_tab, run_mode in ["O"])
        if self.__check_db_continuity(dst_db_writer, run_mode, bgn_date) == 0:
            for trade_date in self.calendar.get_iter_list(bgn_date, stp_date, True):
                df = pd.read_csv(self.__get_date_file_path(trade_date))
                dst_db_writer.update_by_date(trade_date, df, t_using_index=False)
        dst_db_writer.close()
        return 0
