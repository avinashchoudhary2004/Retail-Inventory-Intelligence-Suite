import joblib
import os
import pandas as pd
from pathlib import Path
import numpy as np
import logging

features = [
    'demand_forecast',
    'price',
    'discount',
    'is_promotion_active',
    'weather_condition',
    'competitor_price',
    'seasonality'
]

class ModelPredictor:

    def __init__(self, model_dir='models'):
        self.model_dir = model_dir
        self.model = None
        self.label_encoder = None
        self.threshold = None
        self.n_classes = None
        self.load_artifacts()

    def load_artifacts(self):
        model_path = f"{self.model_dir}/model.pkl"
        label_path = f"{self.model_dir}/label_encoder.pkl"
        threshold_path = f"{self.model_dir}/threshold.pkl"
        nclass_path = f"{self.model_dir}/n_classes.pkl"

        if not os.path.exists(model_path):
            raise FileNotFoundError("model.pkl not found")

        self.model = joblib.load(model_path)

        if os.path.exists(label_path):
            self.label_encoder = joblib.load(label_path)
        else:
            raise FileNotFoundError("label_encoder.pkl not found")

        if os.path.exists(nclass_path):
            self.n_classes = joblib.load(nclass_path)
        else:
            raise FileNotFoundError("n_classes.pkl not found")

        if os.path.exists(threshold_path):
            self.threshold = joblib.load(threshold_path)
            logging.info(f"✓ Binary threshold loaded: {self.threshold}")
        else:
            self.threshold = None
            logging.info("ℹ No threshold.pkl found")

        logging.info("✓ Model artifacts successfully loaded")

    def predict(self, df):
        df_processed = df.copy()
        df_processed.rename(columns={
            'selling_price':'price',
            'discount_applied':'discount',
            'season':'seasonality'
        }, inplace=True)

        try:
            X = df_processed[features]

            if self.n_classes == 2:
                if not hasattr(self.model, "predict_proba"):
                    raise RuntimeError("Model does not support predict_proba for binary thresholding")

                probs = self.model.predict_proba(X)[:, 1]
                thr = self.threshold if self.threshold is not None else 0.5
                preds = (probs >= thr).astype(int)

            else:
                probs = None
                preds = self.model.predict(X)

            preds = self.label_encoder.inverse_transform(preds)

            # print min and max prediction probabilities if available
            if probs is not None:
                logging.info(f"✓ Prediction probabilities range: min={probs.min():.4f}, max={probs.max():.4f}")

            


            # values count of different classes in preds using 
            unique, counts = np.unique(preds, return_counts=True)
            logging.info(f"✓ Prediction completed. Class distribution: {dict(zip(unique, counts))}")

            return preds, probs

        except Exception as e:
            logging.error(f"✗ Prediction failed: {e}")
            raise

    def predict_from_file(self, file_path, output_path=None):
        df = pd.read_csv(file_path)
        preds, probs = self.predict(df)

        df_out = df.copy()
        df_out['forecast_accuracy_flag'] = preds

        # if probs is not None:
        #     df_out['forecast_accuracy_probability'] = probs

        if output_path:
            df_out.to_csv(output_path, index=False)
            logging.info(f"✓ Results saved to {output_path}")

        return df_out


def main():
        data_dir = Path(__file__).parent.resolve() / ".." / "dashboard" / "data"
        input_file = data_dir / "next_7d_demand.csv"

        predictor = ModelPredictor(model_dir="models")

        if input_file.exists():
            res = predictor.predict_from_file(input_file)
            logging.info(res.head())
        else:
            logging.warning("⚠ Input file not found:", input_file)


if __name__ == "__main__":
    main()
