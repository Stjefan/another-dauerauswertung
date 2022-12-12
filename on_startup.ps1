
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py ./services/fun_with_dauerauswertung.py'
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py manage.py runserver'
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py manage.py runscript file_watch'
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py ./services/ueberschreitung.py'

Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py manage.py runscript process_svantek_file'

Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py ./services/fun_with_dauerauswertung.py --modus single --jahr 2022 --monat 10 --tag 24 --stunde 6 --projektbezeichnung "mannheim"'


Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py ./services/delete_duplicates.py'

Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py ./services/fun_with_dauerauswertung.py --modus monat --jahr 2022 --monat 11'
Start-Process cmd -ArgumentList '/K cd C:\Program Files\InfluxData\influxdb\influxdb2_windows_amd64 & .\influxd.exe'
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung'