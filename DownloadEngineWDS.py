import pandas as pd
import mysql.connector


def split_time_window_to_sub_windows(bgn_date: str, stp_date: str) -> list[tuple[str, str]]:
    """

    :param bgn_date:
    :param stp_date:
    :return: split the date period into serval sub periods, make
    """
    _sub_windows = []
    _iter_bgn_date = bgn_date
    _iter_stp_date = str(int(_iter_bgn_date[0:4]) + 1) + "0101"
    while _iter_stp_date < stp_date:
        _sub_windows.append((_iter_bgn_date, _iter_stp_date))
        _iter_bgn_date = _iter_stp_date
        _iter_stp_date = str(int(_iter_bgn_date[0:4]) + 1) + "0101"
    _sub_windows.append((_iter_bgn_date, stp_date))
    return _sub_windows


class CDownloadEngineWDS(object):
    def __init__(self, host: str, user: str, passwd: str, database: str):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.database = database
        self.pDataBase = mysql.connector.connect(
            host=self.host,  # port = 3306
            user=self.user,
            passwd=self.passwd,
            database=self.database
        )

    def download_futures_calendar(self, bgn_date: str, stp_date: str, exchange_id: str) -> pd.DataFrame:
        cursor_object = self.pDataBase.cursor()
        filter_for_dates = "TRADE_DAYS >= '{}' and TRADE_DAYS < '{}' and  S_INFO_EXCHMARKET = '{}'".format(
            bgn_date, stp_date, exchange_id)
        cmd_query = "SELECT TRADE_DAYS FROM CFUTURESCALENDAR WHERE {}".format(filter_for_dates)
        cursor_object.execute(cmd_query)
        download_data = cursor_object.fetchall()

        df = pd.DataFrame(download_data, columns=["trade_date"]).sort_values(by="trade_date", ascending=True)
        print("downloaded dates:")
        print(df)
        print("=" * 24)
        return df

    def download_futures_md_by_date(self, trade_date: str, download_values: list[str], sort_var: str = "S_INFO_WINDCODE") -> pd.DataFrame:
        cursor_object = self.pDataBase.cursor()
        filter_for_dates = "TRADE_DT = {} AND FS_INFO_TYPE = '2'".format(trade_date)
        dfs_list = []
        for source_tab_name in ["CCOMMODITYFUTURESEODPRICES", "CINDEXFUTURESEODPRICES", "CBONDFUTURESEODPRICES"]:
            cmd_query = "SELECT {0} FROM {1} WHERE {2}".format(", ".join(download_values), source_tab_name, filter_for_dates)
            cursor_object.execute(cmd_query)
            download_data = cursor_object.fetchall()
            _df = pd.DataFrame(download_data, columns=download_values).sort_values(by=sort_var, ascending=True)
            dfs_list.append(_df)
        df = pd.concat(dfs_list, axis=0)
        return df

    def download_futures_stock_by_date(self, trade_date: str, download_values: list[str]) -> pd.DataFrame:
        cursor_object = self.pDataBase.cursor()
        filter_for_dates = "ANN_DATE = {} ".format(trade_date)
        cmd_query = "SELECT {} FROM CFUTURESINSTOCK WHERE {}".format(", ".join(download_values), filter_for_dates)
        cursor_object.execute(cmd_query)
        download_data = cursor_object.fetchall()
        df = pd.DataFrame(download_data, columns=download_values)
        return df

    def download_futures_positions_by_date(self, trade_date: str, download_values: list[str], futures_type: str) -> pd.DataFrame:
        """

        :param trade_date:
        :param download_values:
        :param futures_type: 'C' = commodity, 'E' =  equity index
        :return:
        """
        wds_tab_name = {
            "C": "CCOMMODITYFUTURESPOSITIONS",
            "E": "CINDEXFUTURESPOSITIONS",
        }[futures_type]

        cursor_object = self.pDataBase.cursor()
        filter_for_dates = "TRADE_DT = '{}'".format(trade_date)
        download_values_list = ["TRADE_DT"] + download_values
        cmd_query = "SELECT {} FROM {} WHERE {}".format(", ".join(download_values_list), wds_tab_name, filter_for_dates)
        cursor_object.execute(cmd_query)
        download_data = cursor_object.fetchall()
        df = pd.DataFrame(download_data, columns=download_values_list).sort_values(by=["TRADE_DT", "S_INFO_WINDCODE", "FS_INFO_TYPE", "FS_INFO_RANK"])
        return df

    def close(self):
        self.pDataBase.close()
        return 0
