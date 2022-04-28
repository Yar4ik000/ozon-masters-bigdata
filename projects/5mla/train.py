#!/opt/conda/envs/dsenv/bin/python

import os, sys
import logging
import mlflow

from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
from sklearn.compose import ColumnTransformer

import pandas as pd
from joblib import dump

numeric_features = ["I"+str(i) for i in range(1,14)]
categorical_features = ["C"+str(i) for i in range(1, 27)] + ['day_number']

fields = ['id', "label"] + numeric_features

numeric_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='median')),
    ('scaler', StandardScaler())
])

categorical_transformer = Pipeline(steps=[
    ('imputer', SimpleImputer(strategy='constant', fill_value='missing'))
])

preprocessor = ColumnTransformer(
    transformers=[
        ('num', numeric_transformer, numeric_features)
    ]
)

model = Pipeline(steps=[
    ('preprocessor', preprocessor),
    ('logisticregression', LogisticRegression(C=int(sys.argv[2])))
])

#
# Logging initialization
#

#
# Read script arguments
#


#
# Read dataset
#
#fields = """doc_id,hotel_name,hotel_url,street,city,state,country,zip,class,price,
#num_reviews,CLEANLINESS,ROOM,SERVICE,LOCATION,VALUE,COMFORT,overall_ratingsource""".replace("\n",'').split(",")

read_table_opts = dict(sep='\t', names=fields, index_col=False)
df = pd.read_table(sys.argv[1], **read_table_opts)


with mlflow.start_run(run_name="Homework"):
    model.fit(df.drop(columns='label'), df['label'])
    mlflow.sklearn.log_model(model, "model")

