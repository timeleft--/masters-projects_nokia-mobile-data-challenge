cls
FOR %%C IN (c1 c2 c3 c4 c5 c6 c7 c8 c9 c10) DO FOR %%T IN (1) DO START "%%C-%%T" /DC:\mdc-datasets\svmlight\ "cmd /K  fold.bat %%C %%T "