import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import joblib
import json
import boto3
import datetime
import argparse
now_date = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

def label_message(row):
    critical_cols = [c for c in row.index if c.startswith("has_")]
    return "critical" if any(row[c] == 1 for c in critical_cols) else "non_critical"

def read_data(file_path):
    df = pd.read_parquet(file_path)

    return df

def label_data(df):
    df["label"] = df.apply(label_message, axis=1)
    return df

def split_data(df):
    X = df['clean_text']
    y = df['label']
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.3, random_state=2025)
    return X_train, X_test, y_train, y_test

def create_pipeline():
    pipeline = Pipeline([
    ('tfidf', TfidfVectorizer(max_features=500)),
    ('clf', LogisticRegression(max_iter=100, class_weight='balanced', random_state=2025))
    ])
    return pipeline

def train_model(X_train, y_train):
    pipeline = create_pipeline()
    pipeline.fit(X_train, y_train)
    return pipeline

def evaluate_model(pipeline, X_test, y_test):
    y_pred = pipeline.predict(X_test)
    report = classification_report(y_test, y_pred, output_dict=True)

    # Guardar metricas local
    metrics_path = f"Entrenamiento/metrics_{now_date}.json"
    with open(metrics_path, 'w') as f:
        json.dump(report, f)
    return report

def save_model(pipeline):
    local_model_path = f"Entrenamiento/model_{now_date}.joblib"
    joblib.dump(pipeline, local_model_path)
    
    return local_model_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Train a model.')
    parser.add_argument('--input', type=str, required=True, help='Input file path')
    args = parser.parse_args()

    # Leer datos
    print("Reading data...")
    df = read_data(args.input)
    print("Data read successfully.")

    # Etiquetar datos
    print("Labeling data...")
    df = label_data(df)
    print("Data labeled successfully.")

    # Dividir datos
    print("Splitting data...")
    X_train, X_test, y_train, y_test = split_data(df)
    print("Data split successfully.")

    # Entrenar modelo
    print("Training model...")
    pipeline = train_model(X_train, y_train)
    print("Model trained successfully.")

    # Evaluar modelo
    print("Evaluating model...")
    report = evaluate_model(pipeline, X_test, y_test)
    print("Model evaluation completed.")


    # Guardar modelo
    print("Saving model...")
    model_path = save_model(pipeline)
    print(f"Model saved at {model_path}.")


