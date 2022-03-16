add file ozon-masters-bigdata/projects/2/predict.py
add file ozon-masters-bigdata/2.joblib
insert into hw2_pred select transform(id, if1, if2, if3, if4, if5, if6, if7, if8, if9, if10, if11, if12, if13) using 'predict.py' as id, pred from hw2_test where if1 > 20 and if1 < 40;

