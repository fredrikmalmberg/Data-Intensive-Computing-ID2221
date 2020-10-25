from cassandra.cluster import Cluster
import tensorflow as tf
import numpy as np
import pandas as pd
from threading import Timer
import itertools


def build_model():
    model = tf.keras.Sequential([
        tf.keras.layers.Input(shape=(20,)),
        tf.keras.layers.Dense(32, activation='relu', kernel_regularizer=tf.keras.regularizers.l1_l2()),
        tf.keras.layers.Dropout(0.3),
        tf.keras.layers.Dense(16, activation='relu', kernel_regularizer=tf.keras.regularizers.l2()),
        tf.keras.layers.Dropout(0.3),
        tf.keras.layers.Dense(8, activation='relu'),
        tf.keras.layers.Dropout(0.3),
        tf.keras.layers.Dense(4, activation='relu')
    ])

    model.compile(
        optimizer="Adam",
        loss="mean_squared_error",
        metrics=["mean_squared_error"]
    )
    return model

def process_data(df):
    data = df[df["input"].notna() & (df["input"].dropna().map(len) == 5)] 
    x, y = data["input"], data["target"]
    x_predict = np.array([z for z in x.map(lambda x: list(itertools.chain.from_iterable(x)))])
    x_train = x_predict[y.notna()]
    y_train = np.array([z for z in y.dropna()])
    n = len(x_train)
    shuffle = np.arange(n)
    np.random.shuffle(np.arange(n))
    x_train, x_test = x_train[shuffle][:4 * n // 5], x_train[shuffle][4 * n // 5:]
    y_train, y_test = y_train[shuffle][:4 * n // 5], y_train[shuffle][4 * n // 5:]
    return x_predict, x_train, y_train, x_test, y_test

def train(model, x_train, y_train):
    return model.fit(
        x_train, 
        y_train,
        batch_size=20,
        epochs=20,
        verbose=0
    )

def fetch_df(session):
    return session.execute("SELECT * FROM tweets")._current_rows

def main_task(session, model):
    session.row_factory = lambda colnames, rows: pd.DataFrame(rows, columns=colnames)
    df = fetch_df(session)
    x_predict, x_train, y_train, x_test, y_test = process_data(df)
    if len(x_train) > 0:
        if model is None:
            model = build_model()
        else :
            pred = model.predict(x_predict)
        history = train(model, x_train, y_train)
        print(f"evaluating model: {model.evaluate(x_test, y_test)}\n")
    Timer(20, lambda: main_task(session, model)).start()
    

def main():
    session = Cluster(["127.0.0.1"]).connect()
    session.set_keyspace("tweets_space")
    model = None
    main_task(session, model)

if __name__ == "__main__":
    main()