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
from sklearn.metrics import log_loss

import pandas as pd
from joblib import dump

#def main():
print(sys.argv[1], sys.argv[2])
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
        ('logisticregression', LogisticRegression(C=float(sys.argv[3])))
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
df = pd.read_table(sys.argv[2], **read_table_opts)

#X_train, X_test, y_train, y_test = train_test_split(
#        df.drop(columns='label'), df['label'], test_size=0.33, random_state=42)
X_train = df.drop(columns='label')
y_train = df['label']

model.fit(X_train, y_train)
prob_preds = model.predict_proba(X_train)[:, 1]

loss = log_loss(y_train, prob_preds)

mlflow.log_metric('log_loss', loss)
mlflow.log_param('model_param1', float(sys.argv[3])
mlflow.sklearn.log_model(model, artifact_path='model')
#with mlflow.start_run(run_name="Homework"):
#    model.fit(df.drop(columns='label'), df['label'])
#    mlflow.sklearn.log_model(model, "model")

#if __name__ == '__main__':
#    main()
