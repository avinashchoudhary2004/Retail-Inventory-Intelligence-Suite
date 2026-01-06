import sys
import pandas as pd
import joblib

model = joblib.load('C:\\Users\\hp\\Desktop\\ML MODEL\\forecast_accuracy_model.pkl')
encoders = joblib.load('C:\\Users\\hp\\Desktop\\ML MODEL\\feature_encoders.pkl')
target_encoder = joblib.load('C:\\Users\\hp\\Desktop\\ML MODEL\\target_encoder.pkl')

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
new_data.to_csv('C:\\Users\\hp\\Desktop\\ML MODEL\\predictions_output.csv', index=False)
print(new_data[['forecast_predicted_flag']])
