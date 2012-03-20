set OUT_DIR=output
set IN_DIR=input
date /T
time /T
C:\Programs\svm_light\svm_learn.exe -t %3 -x 1 -l %OUT_DIR%\%1\%2\t%3\trans.txt -a %OUT_DIR%\%1\%2\t%3\alpha.txt %IN_DIR%\%1\%2\input.csv %OUT_DIR%\%1\%2\t%3\model.txt 1>svmlight_%1-%2-t%3_train-out.txt 2>svmlight_%1-%2-t%3_train-error.txt
date /T
time /T
C:\Programs\svm_light\svm_classify.exe %IN_DIR%\%1\%2\validate.csv %OUT_DIR%\%1\%2\t%3\model.txt %OUT_DIR%\%1\%2\t%3\predictions.txt  1>svmlight_%1-%2-t%3_test-out.txt 2>svmlight_%1-%2-t%3_test-error.txt
date /T
time /T
