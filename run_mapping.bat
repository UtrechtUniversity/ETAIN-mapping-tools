@echo off
set LOGFILE=logfile.txt

echo Activating virtual environment... >> %LOGFILE% 2>&1
call venv\Scripts\activate.bat >> %LOGFILE% 2>&1

echo Running main.py... >> %LOGFILE% 2>&1
python -u src\main.py >> %LOGFILE% 2>&1

echo Running generate_count_grid.py... >> %LOGFILE% 2>&1
python -u src\generate_count_grid.py >> %LOGFILE% 2>&1

echo Done. >> %LOGFILE% 2>&1
