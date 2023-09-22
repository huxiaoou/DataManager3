$bgn_date_calendar = "20080101"
$bgn_date_md = "20120101"
$today = Get-Date
$stp_date_calendar = Get-Date -Date $today.AddDays(90) -Format "yyyyMMdd"
$stp_date = Read-Host -Prompt "Please input the STOP date, format = [YYYYMMDD]"

# --- calendar
python main.py -w d -t cal    -m o -b $bgn_date_calendar -s $stp_date_calendar

# --- md_wds
python main.py -w d -t md_wds -m o -b $bgn_date_md       -s $stp_date

# --- posc and pose
python main.py -w d -t posc   -m o -b $bgn_date_md       -s $stp_date
python main.py -w d -t pose   -m o -b $bgn_date_md       -s $stp_date

# --- stock
python main.py -w d -t stock  -m o -b $bgn_date_md       -s $stp_date

# --- basis
$ans = Read-Host -Prompt "--- ! CAUTION ! ---
Are you sure you want to overwrite all BASIS data?
This operation will cost huge amount quotes of WindAPI.
Make sure you know exactly what you're doing!
--- ! CAUTION ! ---
Input 'y' to continue, else to abandon"
if ($ans -eq "y")
{
    python main.py -w d -t basis  -m o -b $bgn_date_md       -s $stp_date
}

# sync command example:
# python main.py -w s -b 20120101 -s 20230921 -vs md,cm01,em01,basis

# check command example:
# python main.py -w c -m o -b 20120104 -s 20120201 -vs open,high,low,close,settle,presettle,volume,amount,oi # not to print details if no error
# python main.py -w c -m a -b 20120104 -s 20120201 -vs open,high,low,close,settle,presettle,volume,amount,oi # to print details if no error