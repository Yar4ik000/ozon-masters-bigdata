add file ozon-masters-bigdata/projects/2/predict.py
add file ozon-masters-bigdata/2.joblib
insert into hw2_pred 
select transform(*)
using 'predict.py' as (id, pred) 
from hw2_test where if1 > 20 and if1 < 40;

