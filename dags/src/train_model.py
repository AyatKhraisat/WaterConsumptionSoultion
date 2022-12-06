import os
from datetime import time

import joblib
import mlflow
import numpy as np
import pandas as pd
from airflow import AirflowException
from pmdarima.arima import auto_arima
from statsmodels.tsa.statespace.sarimax import SARIMAX


def get_data(data_path):
    print(data_path)
    try:
        df_main = pd.read_csv(data_path)
    except Exception as e:
        print(e)
        raise AirflowException('no data to process')
    return process(df_main.copy())



def process(df):
    mlflow.set_tracking_uri("http://mlflow:5000")
    df = df.drop('AccountOID', axis=1)
    dates = sorted(df['ConsumptionDate'].dropna().to_list())
    all_dates = pd.date_range(dates[0], dates[-1])
    all_dates_df = pd.DataFrame(all_dates, columns=['ConsumptionDate'])
    df['ConsumptionDate'] = pd.to_datetime(df['ConsumptionDate'])
    df = pd.merge(all_dates_df, df, on='ConsumptionDate', how='left')
    df['Consumption'] = df['Consumption'].fillna(0)
    df["ConsumptionDate"] = pd.to_datetime(df["ConsumptionDate"])
    df.loc[df['Consumption'] > 800, 'Consumption'] = df['Consumption'].median()
    df = df.set_index('ConsumptionDate')

    return df


def modeling(dataset,model_path):
    import mlflow
    if dataset is None:
        print('no data to process')
        return
    data_size = dataset.shape[0]
    test_size = 30
    train_X, train_y = dataset[:data_size - test_size], dataset[:data_size - test_size]
    test_X, test_y = dataset[data_size - test_size:], dataset[data_size - test_size:]

    with mlflow.start_run() as run:
        # Automating ARIMA to choose best parameters
        step_wise = auto_arima(train_y, exogenous=train_X, seasonal=True, start_p=1, start_q=1, max_p=7, max_q=7,
                               error_action='ignore', suppress_warnings=True, stepwise=True, parallel=True,
                               verbose=False)
        mlflow.log_param("step_wise.order", step_wise.order)
        # SARIMAX modeling with the choosen parameters
        model = SARIMAX(train_y, exog=train_X, order=step_wise.order, initialization='approximate_diffuse',enforce_stationarity=False)

        results = model.fit()
        # Predictions
        results.predict(start=data_size - test_size, end=data_size - 1, exog=np.array(test_X))
        mlflow.log_metric("aic", results.aic)
        mlflow.set_tag('status', 'the inital model ')

        mlflow.sklearn.log_model(
            model, "SARIMAX"
        )
        joblib.dump(results, model_path)

    mlflow.end_run()
    print(mlflow.get_run(run_id=run.info.run_id))




def remodeling(dataset,model_path):
    import mlflow
    if dataset is None:
        print('no data to process')
        return
    data_size = dataset.shape[0]
    test_size = 30
    train_X, train_y = dataset[:data_size - test_size], dataset[:data_size - test_size]
    test_X, test_y = dataset[data_size - test_size:], dataset[data_size - test_size:]
    old_model = joblib.load(model_path)

    with mlflow.start_run() as run:
        step_wise = auto_arima(train_y, exogenous=train_X, seasonal=True, start_p=1, start_q=1, max_p=7, max_q=7,
                               error_action='ignore', suppress_warnings=True, stepwise=True, parallel=True,
                               verbose=False)
        mlflow.log_param("step_wise.order", step_wise.order)
        model = SARIMAX(train_y, exog=train_X, order=step_wise.order, initialization='approximate_diffuse',enforce_stationarity=False)
        results = model.fit()
        results.predict(start=data_size - test_size, end=data_size - 1, exog=np.array(test_X))
        old_model.predict(start=data_size - test_size, end=data_size - 1, exog=np.array(test_X))
        old_model_aic=old_model.aic
        new_model_aic=results.aic
        print("new_model_aic",new_model_aic)
        print("old_model_aic",old_model_aic)

        mlflow.log_metric("aic",new_model_aic)
        mlflow.sklearn.log_model(
            model, "SARIMAX"
        )

        if new_model_aic<old_model_aic:

            mlflow.set_tag('status', 'the model from this run replaced the current version ')

            joblib.dump(results, model_path)
        else:
            mlflow.set_tag('status', 'the model from this run did not replace the current version ')

    mlflow.end_run()
    print(mlflow.get_run(run_id=run.info.run_id))

def train_model(**kwargs):
    dataset=get_data(kwargs['data_path'])
    modeling(dataset=dataset,
                model_path=kwargs['model_path'])


def retrain_model(**kwargs):
    dataset = get_data(kwargs['data_path'])
    modeling(dataset=dataset,
             model_path=kwargs['current_model_path'])


if __name__ == '__main__':
   retrain_model()