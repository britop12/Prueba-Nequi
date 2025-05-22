import joblib
import pandas as pd
import argparse

def inference(pipeline_path, sample_path):
    """
    Loads a trained pipeline and applies it to the sample dataframe.
    
    :param pipeline_path: Path to the trained ML pipeline file
    :param sample_path: Path to the CSV file containing the sample data
    :return: Path to the CSV file with predictions
    """
    # Load the trained pipeline
    pipeline = joblib.load(pipeline_path)
    
    # Load the sample data
    df_sample = pd.read_csv(sample_path)
    
    # Apply the pipeline to the sample data
    preds = pipeline.predict(df_sample['clean_text'])
    df_sample['prediction'] = preds

    output_path = "Entrenamiento/sample_predictions.csv"
    df_sample.to_csv(output_path, index=False)
    
    print(f"Predictions saved to {output_path}")
    return output_path

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Apply trained ML pipeline to sample data.')
    parser.add_argument('--pipeline_path', type=str, required=True, help='Path to the trained pipeline file (.joblib)')
    parser.add_argument('--sample_path', type=str, required=True, help='Path to the sample CSV file')

    args = parser.parse_args()
    inference(args.pipeline_path, args.sample_path)
