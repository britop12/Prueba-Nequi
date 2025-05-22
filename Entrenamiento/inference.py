import joblib
import pandas as pd

pipeline = joblib.load("Entrenamiento\model_20250522_000539.joblib")
df_new = pd.read_csv("Entrenamiento\sample.csv") 

preds = pipeline.predict(df_new['clean_text'])
df_new['prediction'] = preds

df_new.to_csv("Entrenamiento\sample_predictions.csv", index=False)
