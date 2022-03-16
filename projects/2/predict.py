#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')
from model import fields

#load the model
model = load("2.joblib")

for line in sys.stdin:
    line = line.replace('\\N', 'nan')
    (id, if1, if2, if3, if4, if5, if6, if7, if8, if9, if10, if11, if12, if13) = line.split('\t')
    if if1 <= 20 or if1 >= 40:
        continue
    df = [[id, if1, if2, if3, if4, if5, if6, if7, if8, if9, if10, if11, if12, if13]]
    pred = model.predict(df)
    print(f"{id}\t{pred[0]}"]))
