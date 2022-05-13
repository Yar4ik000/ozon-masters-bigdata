#!/opt/conda/envs/dsenv/bin/python


import os, sys
import sklearn
import joblib
import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression


columns = ['id', 'label', 'verified', 'vote']

df = pd.read_parquet(sys.argv[2], columns=columns)
df = df.fillna(0)

logreg = LogisticRegression()
logreg.fit(df[['verified', 'vote']], df['label'])

joblib.dump(logreg, sys.argv[4])
