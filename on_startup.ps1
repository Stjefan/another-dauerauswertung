
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py ./services/fun_with_dauerauswertung.py'
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py manage.py runserver'
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py manage.py runscript file_watch'
Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py ./services/ueberschreitung.py'

Start-Process cmd -ArgumentList '/K cd C:\Users\sts\Documents\GitHub\another-dauerauswertung & .\venv\Scripts\activate & cd .\django_dauerauswertung & py manage.py runscript process_svantek_file'