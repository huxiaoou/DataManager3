$bgn_date_md = "20120101"
$user_choice = Read-Host -Prompt "Please choose which part to run 'download/sync/check/translate', [d/s/c/t]"
$stp_date = Read-Host -Prompt "Please input the STOP date, format = [YYYYMMDD]"
if ($user_choice -eq "d")
{
    $today = Get-Date
    $bgn_date_calendar = "20080101"
    $stp_date_calendar = Get-Date -Date $today.AddDays(90) -Format "yyyyMMdd"

    python main.py -w d -t cal    -m o -b $bgn_date_calendar -s $stp_date_calendar
    python main.py -w d -t md_wds -m o -b $bgn_date_md       -s $stp_date
    python main.py -w d -t posc   -m o -b $bgn_date_md       -s $stp_date
    python main.py -w d -t pose   -m o -b $bgn_date_md       -s $stp_date
    python main.py -w d -t stock  -m o -b $bgn_date_md       -s $stp_date
    $ans = Read-Host -Prompt "
--- ! CAUTION ! ---
Are you sure you want to overwrite all BASIS data?
This operation will cost huge amount quotes of WindAPI.
Make sure you know exactly what you're doing!
--- ! CAUTION ! ---
Input 'y' to continue, else to abandon"
    if ($ans -eq "y")
    {
        python main.py -w d -t basis  -m o -b $bgn_date_md       -s $stp_date
    }
}
elseif ($user_choice -eq "s") # sync
{
    python main.py -w s -b $bgn_date_md -s $stp_date -vs md,basis
#    python main.py -w s -b $bgn_date_md -s $stp_date -vs cm01,em01
}
elseif ($user_choice -eq "c") # check
{
    python main.py -w c -m o -b $bgn_date_md -s $stp_date -vs open,high,low,close,settle,presettle,volume,amount,oi # not to print details if no error
}
elseif ($user_choice -eq "t") # translate
{
    python main.py -w t -t md_wds  -m o -b $bgn_date_md -s $stp_date
    python main.py -w t -t md_tsdb -m o -b $bgn_date_md -s $stp_date
    python main.py -w t -t posc    -m o -b $bgn_date_md -s $stp_date
    python main.py -w t -t pose    -m o -b $bgn_date_md -s $stp_date
    python main.py -w t -t stock   -m o -b $bgn_date_md -s $stp_date
    python main.py -w t -t basis   -m o -b $bgn_date_md -s $stp_date
    #    python main.py -w t -t cm01    -m o -b $bgn_date_md -s $stp_date
    #    python main.py -w t -t em01    -m o -b $bgn_date_md -s $stp_date

    # Not available on Windows
    # codes are just recorded here to backup
    # python main.py -w t -t rf -m o -b 20140701 -s 20231017
    # python main.py -w t -t rd -m o -b 20140701 -s 20231017
    # python main.py -w t -t nf -m o -b 20140701 -s 20231017
    # python main.py -w t -t nd -m o -b 20140701 -s 20231017
}
