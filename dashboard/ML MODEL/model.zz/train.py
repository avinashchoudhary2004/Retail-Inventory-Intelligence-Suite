import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from xgboost import XGBClassifier
from sklearn.metrics import classification_report, confusion_matrix, accuracy_score
import joblib

# 🔹 1. Load Dataset
df = pd.read_csv('Forecast Deviation.csv')

# 🔹 2. Target column
target = 'forecast_accuracy_flag'
y = df[target]

# 🔧 Encode target column
target_encoder = LabelEncoder()
y = target_encoder.fit_transform(y)  # Converts classes to 0, 1, 2

# 🔹 3. Features
features = [
    'demand_forecast',
    'price',
    'discount',
    'Holiday_Promotion',
    'weather_condition',
    'seasonality'
]

X = df[features].copy()

# 🔧 Encode categorical columns
cat_cols = ['Holiday_Promotion', 'weather_condition', 'seasonality']
encoders = {}
for col in cat_cols:
    le = LabelEncoder()
    X[col] = le.fit_transform(X[col].astype(str))
    encoders[col] = le

# 🔧 Split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# 🔧 Models
models = {
    "XGBoost": XGBClassifier(use_label_encoder=False, eval_metric='mlogloss', random_state=42)
}

scores = []
best_model = None
best_score = 0

# 🔧 Fit and evaluate all models
for name, model in models.items():
    model.fit(X_train, y_train)
    y_pred_num = model.predict(X_test)
    y_pred = target_encoder.inverse_transform(y_pred_num)  # decode target
    acc = accuracy_score(y_test, y_pred_num)
    scores.append({"Model": name, "Accuracy": acc, "model": model, "y_pred_num": y_pred_num})
    print(f"\n{name}:\n{classification_report(target_encoder.inverse_transform(y_test), y_pred)}")
    if acc > best_score:
        best_score = acc
        best_model = model

print(f"\n🏆 Best Model: {best_model.__class__.__name__} with Accuracy = {best_score:.2%}")

# 💾 Save model & encoders
joblib.dump(best_model, 'forecast_accuracy_model.pkl')
joblib.dump(encoders, 'feature_encoders.pkl')
joblib.dump(target_encoder, 'target_encoder.pkl')

# 📄 Save predictions
df['forecast_predicted_flag'] = target_encoder.inverse_transform(best_model.predict(X))
df.to_csv('Forecast_Deviation_with_predictions.csv', index=False)

# 📊 Plot 1: Confusion Matrix for best model
best_model_y_pred_num = best_model.predict(X_test)
cm = confusion_matrix(y_test, best_model_y_pred_num)

plt.figure(figsize=(6,4))
sns.heatmap(
    cm, annot=True, fmt='d', cmap='Blues',
    xticklabels=target_encoder.classes_,
    yticklabels=target_encoder.classes_
)
plt.title(f'Confusion Matrix ({best_model.__class__.__name__})')
plt.ylabel('True')
plt.xlabel('Predicted')
plt.tight_layout()
plt.savefig('confusion_matrix.png')
plt.show()

# 📊 Plot 2: Training vs Test accuracy per model
model_names = []
train_accuracies = []
test_accuracies = []
for s in scores:
    model_names.append(s['Model'])
    train_acc = accuracy_score(y_train, s['model'].predict(X_train))
    train_accuracies.append(train_acc)
    test_accuracies.append(s['Accuracy'])

plt.figure(figsize=(8,5))
x_pos = range(len(model_names))
plt.bar(x_pos, train_accuracies, width=0.4, label='Train', color='#2ca02c')
plt.bar([p + 0.4 for p in x_pos], test_accuracies, width=0.4, label='Test', color='#1f77b4')
plt.xticks([p + 0.2 for p in x_pos], model_names, rotation=45, ha='right')
plt.ylabel('Accuracy')
plt.title('Training vs Test Accuracy by Model')
plt.legend()
plt.tight_layout()
plt.savefig('train_test_accuracy.png')
plt.show()

print("\n✅ Plots saved as 'confusion_matrix.png' and 'train_test_accuracy.png'")
