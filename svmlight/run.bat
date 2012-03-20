cls
set BASE_DIR=C:\mdc-datasets\svmlight\
FOR /F %%C IN ('dir /b %BASE_DIR%\input\c*') DO FOR %%T IN (1) DO START "%%C:Kernel%%T" /D%BASE_DIR% "cmd /K  fold.bat %%C %%T "