#!/opt/conda/envs/dsenv/bin/python

import sys, os
import logging
from joblib import load
import pandas as pd

sys.path.append('.')
from model import fields

#load the model
model = load("2.joblib")
fields.remove('label')

read_opts = dict(
        sep='\t', names=fields, index_col=False, header=None,
        iterator=True, chunksize=500
)

for df in pd.read_csv(sys.stdin, **read_opts):
    df = df.replace({'\\N': None})
    pred = model.predict(df)
    out = zip(df.id, pred)
    print('\n'.join(["{0}\t{1}".format(*i) for i in out]))
