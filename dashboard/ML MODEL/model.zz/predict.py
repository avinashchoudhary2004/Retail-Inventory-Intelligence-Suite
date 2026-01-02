import sys
import pandas as pd
import joblib
import os
from pathlib import Path

# Get the directory where this script is located
script_dir = Path(__file__).parent

# Define model paths relative to script location
model_path = script_dir / 'forecast_accuracy_model.pkl'
encoders_path = script_dir / 'feature_encoders.pkl'
target_encoder_path = script_dir / 'target_encoder.pkl'

# Load models with error handling
try:
    model = joblib.load(model_path)
    encoders = joblib.load(encoders_path)
    target_encoder = joblib.load(target_encoder_path)
    print(f"Models loaded successfully from {script_dir}")
except FileNotFoundError as e:
    print(f"Error loading models: {e}")
    print(f"Looking for files in: {script_dir}")
    print(f"Files in directory: {list(script_dir.glob('*'))}")
    sys.exit(1)

if len(sys.argv) > 1:
    file_path = sys.argv[1]
    new_data = pd.read_csv(file_path)
else:
    new_data = pd.DataFrame([{
        "demand_forecast": 500,
        "price": 29.99,
        "discount": 10,
        "Holiday_Promotion": "Yes",
        "weather_condition": "Sunny",
        "seasonality": "Summer"
    }])

cat_cols = ['Holiday_Promotion', 'weather_condition', 'seasonality']
new_data[cat_cols] = new_data[cat_cols].apply(lambda df: df.astype(str).str.lower())
new_data['Holiday_Promotion'] = new_data['Holiday_Promotion'].map({'yes': 1, 'no': 0}).fillna(new_data['Holiday_Promotion'])
new_data['Holiday_Promotion'] = new_data['Holiday_Promotion'].astype(str)

for col in cat_cols:
    le = encoders[col]
    known_classes = list(le.classes_)
    new_data[col] = new_data[col].apply(lambda val: val if val in known_classes else known_classes[0])
    new_data[col] = le.transform(new_data[col])

y_pred_num = model.predict(new_data)
y_pred_label = target_encoder.inverse_transform(y_pred_num)

new_data['forecast_predicted_flag'] = y_pred_label

# Save output to script directory
output_path = script_dir / 'predictions_output.csv'
new_data.to_csv(output_path, index=False)
print(new_data[['forecast_predicted_flag']])