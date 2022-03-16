add local file predict.py
insert into hw2_pred select transform(*) using 'predict.py' as (id,pred) from hw2_test;
