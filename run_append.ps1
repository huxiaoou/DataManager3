$user_choice = Read-Host -Prompt "Please choose which part to run 'download/translate', [d/t]"
$append_date = Read-Host -Prompt "Please input the APPEND date, format = [YYYYMMDD]"

if ($user_choice -eq "d")
{
    Write-Host "... user choose to run DOWNLOAD"

    python main.py -w d -t cal    -m a -b $append_date -bs 10 -ss 60
    python main.py -w d -t md_wds -m a -b $append_date
    python main.py -w d -t posc   -m a -b $append_date
    python main.py -w d -t pose   -m a -b $append_date
    python main.py -w d -t stock  -m a -b $append_date
    python main.py -w d -t basis  -m a -b $append_date

    # --- sync and check
    python main.py -w s -b $append_date -vs md,basis
#    python main.py -w s -b $append_date -vs cm01,em01
    python main.py -w c -m a -b $append_date -vs open,high,low,close,settle,presettle,volume,amount,oi
}
elseif ($user_choice -eq "t")
{
    Write-Host "... user choose to run TRANSLATE"

    # --- translate
    python main.py -w t -t md_wds  -m a -b $append_date
    python main.py -w t -t md_tsdb -m a -b $append_date
    python main.py -w t -t posc    -m a -b $append_date
    python main.py -w t -t pose    -m a -b $append_date
    python main.py -w t -t stock   -m a -b $append_date
    python main.py -w t -t basis   -m a -b $append_date
    #python main.py -w t -t cm01    -m a -b $append_date
    #python main.py -w t -t em01    -m a -b $append_date

    # Not available on Windows
    # codes are just recorded here to backup
    # python main.py -w t -t rf -m a -b 20231017
    # python main.py -w t -t rd -m a -b 20231017
    # python main.py -w t -t nf -m a -b 20231017
    # python main.py -w t -t nd -m a -b 20231017
}

