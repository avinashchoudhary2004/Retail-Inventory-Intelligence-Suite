import os
import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from train import main as train_model
from predict import ModelPredictor

def run_ml_pipeline(target_date = None):
    logging.info("="*60)
    logging.info("STARTING COMPLETE ML PIPELINE")
    logging.info("="*60)
    
    base_dir = Path(__file__).parent.parent
    dashboard_data_dir = base_dir / "dashboard" / "data"
    model_dir = Path(__file__).parent / "models"
    
    # STEP 1: Train Model if target date is last date of month 
    logging.info("\n[STEP 1] Training Model...")
    logging.info("-" * 60)

    try:
        is_empty = not any(os.scandir(model_dir))

        if target_date or is_empty:
            model_obj, model_name, label_encoder,   n_classes = train_model(
                data_path=dashboard_data_dir / 'forecast_deviation.csv',
                model_dir=str(model_dir)
            )

            logging.info("✓ Model training completed successfully")
            logging.info(f"✓ Model saved to: {model_dir}/")
            logging.info(f"✓ Selected model: {model_name}")

            if model_obj.get("threshold") is not None:
                logging.info(f"✓ Decision threshold: {model_obj['threshold']:.4f}")
            else:
                logging.info("✓ Multi-class classifier: no threshold applied")

    except Exception as e:
        logging.error(f"✗ Model training failed: {e}")
        import traceback; traceback.print_exc()
        return False
    
    # STEP 2: Make Predictions
    logging.info("\n[STEP 2] Making Predictions on Inference Data...")
    logging.info("-" * 60)

    try:
        predictor = ModelPredictor(model_dir=str(model_dir))
        inference_file = dashboard_data_dir / "next_7d_demand.csv"

        if not inference_file.exists():
            logging.error(f"✗ No inference data found at {inference_file}")
            return False
        
        output_file = dashboard_data_dir / "next_7d_demand_classification.csv"
        results_df = predictor.predict_from_file(
            file_path=str(inference_file),
            output_path=str(output_file)
        )

        logging.info(f"✓ Predictions made for {len(results_df)} records")

    except Exception as e:
        logging.error(f"✗ Prediction failed: {e}")
        import traceback; traceback.print_exc()
        return False

    # Summary
    logging.info("\n" + "="*60)
    logging.info("✓ ML PIPELINE COMPLETED SUCCESSFULLY")
    logging.info("="*60)
    
    logging.info("\nResults Summary:")
    logging.info(f"  - Model used: {model_name}")
    logging.info(f"  - Predictions: {len(results_df)} rows")
    logging.info(f"  - Saved to: {output_file}")

    if "forecast_accuracy_flag" in results_df.columns:
        logging.info("\nPrediction Distribution:")
        logging.info(results_df["forecast_accuracy_flag"].value_counts().to_dict())

    logging.info("\nSample output:")
    logging.info(results_df.head())

    return True


if __name__ == "__main__":
    success = run_ml_pipeline()
    sys.exit(0 if success else 1)
