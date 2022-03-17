#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')

#load the model
model = load("2.joblib")
fields = ['id'] + ['if' + str(i) for i in range(1, 14)]

read_opts = dict(
        sep='\t', names=fields, index_col=False, header=None,
        iterator=True, chunksize=500
)

for df in pd.read_csv(sys.stdin, **read_opts):
    df = df.replace('\\N', 0)
    pred = model.predict(df)
    out = zip(df.id, pred)
    print('\n'.join(["{0}\t{1}".format(*i) for i in out]))
