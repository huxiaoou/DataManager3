import argparse
import re
import pandas as pd
from DownloadEngineWDS import CDownloadEngineWDS
from project_setup import global_config

parser = argparse.ArgumentParser(description="A python version interface to view WDS.")
parser.add_argument("-v", "--values", type=str, help="Use ',' to split multiple options, for example: 'value,tag', no space is needed.")
parser.add_argument("-d", "--date", type=str, help="format = YYYYMMDD")
parser.add_argument("-m", "--maxrows", type=int, default=12, help="Maximum rows to be displayed")
parser.add_argument("-t", "--ticker", type=str, default="",
                    help="""Use this to filter ticker. Regular expressions are support, such as g305, '^ag', '3$', 'j[\d]'. 
                         You may need to keep the single quote ' to this expression to make the argument work. This expression is case sensitive.""")

rename_mapper = {
    "contract": "S_INFO_WINDCODE",
    "settle": "S_DQ_SETTLE",
    "presettle": "S_DQ_PRESETTLE",
    "open": "S_DQ_OPEN",
    "high": "S_DQ_HIGH",
    "low": "S_DQ_LOW",
    "close": "S_DQ_CLOSE",
    "volume": "S_DQ_VOLUME",
    "amt": "S_DQ_AMOUNT",
    "oi": "S_DQ_OI",
    "ret": "S_DQ_CHANGE",
}
reverse_rename_mapper = {v: k for k, v in rename_mapper.items()}

args = parser.parse_args()
download_values = [rename_mapper[_] for _ in (["contract"] + args.values.split(","))]
trade_date = args.date
filter_ticker = args.ticker

print("-" * 120)
print("date             = {:>90s}".format(trade_date))
print("values           = {:>90s}".format(args.values))
print("max display rows = {:>90s}".format(str(args.maxrows)))
print("ticker           = {:>90s}".format(args.ticker))

pd.set_option("display.width", 0)
pd.set_option("display.max_rows", args.maxrows)
pd.set_option("display.float_format", "{:.4f}".format)

downloadMgr = CDownloadEngineWDS(
    host=global_config["account"]["wds"]["host"],
    user=global_config["account"]["wds"]["user"],
    passwd=global_config["account"]["wds"]["passwd"],
    database=global_config["account"]["wds"]["database"],
)

df = downloadMgr.download_futures_md_by_date(trade_date=trade_date, download_values=download_values)
if filter_ticker != "":
    filter_data = df["S_INFO_WINDCODE"].map(lambda z: re.search(filter_ticker.upper(), z) is not None)
    selected_df = df.loc[filter_data].copy()
else:
    selected_df = df
selected_df.rename(mapper=reverse_rename_mapper, axis=1, inplace=True)
after_drop_df = selected_df.dropna(axis=0, how="all")

print("-" * 120)
print(selected_df)
print("-" * 120)
print(f"size of selected df              : {len(selected_df)}")
print(f"size of selected df after drop na: {len(after_drop_df)}")
print("-" * 120)
