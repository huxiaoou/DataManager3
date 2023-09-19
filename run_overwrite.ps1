$bgn_date = "20080101"
$stp_date = Read-Host -Prompt "Please input the stp date, formant = [YYYYMMDD]"
python main.py -w d -t cal -m o -b $bgn_date -s $stp_date