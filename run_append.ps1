$append_date = Read-Host -Prompt "Please input the APPEND date, format = [YYYYMMDD]"

# --- calendar
python main.py -w d -t cal    -m a -b $append_date -bs 10 -ss 60

# --- md_wds
python main.py -w d -t md_wds -m a -b $append_date

# --- posc and pose
python main.py -w d -t posc   -m a -b $append_date
python main.py -w d -t pose   -m a -b $append_date

# --- stock and basis
python main.py -w d -t stock  -m a -b $append_date
python main.py -w d -t basis  -m a -b $append_date

# --- sync and check
python main.py -w s -b $append_date -vs md,cm01,em01,basis
python main.py -w c -m a -b $append_date -vs open,high,low,close,settle,presettle,volume,amount,oi
