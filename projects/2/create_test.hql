create temporary external table hw2_test(
	id int, if1 int, if2 int, if3 int, if4 int, if5 int, if6 int, 
	if7 int, if8 int, if9 int, if10 int, if11 int, if12 int, if13 int)
row format delimited
fields terminated by '\t'
stored as textfile
location '/datasets/criteo/criteo_test_large_features';

