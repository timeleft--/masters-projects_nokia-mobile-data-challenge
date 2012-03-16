C:\Programs\svm_light\svm_learn.exe -t %3 -x 1 -l output\%1\%2\t%3\trans.txt -a output\%1\%2\t%3\alpha.txt input\%1\%2\input.csv output\%1\%2\t%3\model.txt 1>%1-%2-t%3_train-out.txt 2>%1-%2-t%3_train-error.txt
C:\Programs\svm_light\svm_classify.exe input\%1\%2\validate.csv output\%1\%2\t%3\model.txt output\%1\%2\t%3\predictions.txt  1>%1-c%2-t%3_test-out.txt 2>%1-%2-t%3_test-error.txt
