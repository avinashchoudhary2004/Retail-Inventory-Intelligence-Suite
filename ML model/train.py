import pandas as pd
import numpy as np
from pathlib import Path
import joblib

from sklearn.model_selection import RandomizedSearchCV
from sklearn.metrics import f1_score, classification_report, accuracy_score
from sklearn.preprocessing import OneHotEncoder, StandardScaler, OrdinalEncoder, LabelEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.svm import LinearSVC
from sklearn.calibration import CalibratedClassifierCV
from xgboost import XGBClassifier
import logging

# ---------------- Paths ----------------
file_path = Path(__file__).parent.resolve()
save_dir = file_path / '..' / "dashboard" / "data"
forecast_horizon = pd.Timedelta(days=50)

# ---------------- Features ----------------
num_features = [
    'demand_forecast',
    'price',
    'discount',
    'competitor_price'
]

cat_features = [
    'is_promotion_active',
    'weather_condition',
    'seasonality'
]

features = num_features + cat_features
target = 'forecast_accuracy_flag'


# ---------------- Load & Process ----------------
def load_data(path):
    return pd.read_csv(path)

def process_data(df):
    deviation_p = df['units_deviation']/df['units_sold']*100

    conditions=[
        (deviation_p>-7) & (deviation_p<15),
        deviation_p>=15,
        deviation_p<=-7
    ]

    choices = ['ACCURATE', 'OVER-FORECAST', 'UNDER-FORECAST']
    df['forecast_accuracy_flag'] = np.select(conditions, choices, default='ACCURATE')

    df.rename(columns={'full_date':'date'}, inplace=True)
    df['date'] = pd.to_datetime(df['date'])
    return df


# ---------------- Train/Test Split ----------------


def split_data(df):
    max_date = df['date'].max()
    split_date = max_date - forecast_horizon

    train = df[df['date'] <= split_date]
    test  = df[df['date'] > split_date]

    return train[features], test[features], train[target], test[target]


# ---------------- Threshold Optimization ----------------
def find_best_threshold(y_true, probs):
    best_t, best_f1 = 0.5, 0
    for t in np.arange(0.2, 0.8, 0.02):
        preds = (probs >= t).astype(int)
        f1 = f1_score(y_true, preds)
        if f1 > best_f1:
            best_f1, best_t = f1, t
    return best_t, best_f1


# ---------------- Build Pipelines ----------------
def build_xgb_pipeline(n_classes):
    pre = ColumnTransformer([
        ('cat', OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1), cat_features),
        ('num', 'passthrough', num_features)
    ])

    if n_classes == 2:
        model = XGBClassifier(
            objective='binary:logistic',
            eval_metric='logloss',
            random_state=42
        )
    else:
        model = XGBClassifier(
            objective='multi:softprob',
            num_class=n_classes,
            eval_metric='mlogloss',
            random_state=42
        )

    return Pipeline([('preprocess', pre), ('model', model)])


def build_svm_pipeline():
    pre = ColumnTransformer([
        ('cat', OneHotEncoder(handle_unknown='ignore'), cat_features),
        ('num', StandardScaler(), num_features)
    ])

    base = LinearSVC(
        class_weight='balanced',
        random_state=42
    )

    calibrated = CalibratedClassifierCV(base, cv=3)

    return Pipeline([('preprocess', pre), ('model', calibrated)])


# ---------------- Hyperparameter Grids ----------------
xgb_param_grid = {
    'model__n_estimators': [200, 400, 800],
    'model__max_depth': [3, 5, 7],
    'model__learning_rate': [0.01, 0.05, 0.1],
    'model__subsample': [0.7, 0.8, 1.0],
    'model__colsample_bytree': [0.7, 0.8, 1.0]
}

svm_param_grid = {
    'model__estimator__C': [0.1, 1, 10]
}


# ---------------- Train & Select ----------------
def train_and_select(X_train, X_test, y_train, y_test):
    # Encode target
    le = LabelEncoder()
    y_train = le.fit_transform(y_train)
    y_test  = le.transform(y_test)

    n_classes = len(np.unique(y_train))
    logging.info(f"Detected classes: {np.unique(y_train)}")

    models = {
        'XGBoost': (build_xgb_pipeline(n_classes), xgb_param_grid),
        'LinearSVM': (build_svm_pipeline(), svm_param_grid)
    }

    results = {}

    for name, (pipeline, param_grid) in models.items():
        logging.info(f"\nTraining {name} with hyperparameter tuning...")

        scoring = 'f1' if n_classes == 2 else 'f1_weighted'

        search = RandomizedSearchCV(
            pipeline,
            param_distributions=param_grid,
            n_iter=3,
            scoring=scoring,
            cv=2,
            n_jobs=-1,
            verbose=2,
            random_state=42
        )
        search.fit(X_train, y_train)
        best = search.best_estimator_

        if n_classes == 2:
            probs = best.predict_proba(X_test)[:, 1]
            threshold, best_f1 = find_best_threshold(y_test, probs)
        else:
            preds = best.predict(X_test)
            threshold = None
            best_f1 = f1_score(y_test, preds, average='weighted')

        results[name] = {
            'pipeline': best,
            'threshold': threshold,
            'f1': best_f1
        }
        logging.info(f"{name} → F1={best_f1:.4f}, threshold={threshold}")

    best_name = max(results, key=lambda k: results[k]['f1'])
    
    # print classification report for best model
    best_model = results[best_name]['pipeline']
    if n_classes == 2:
        best_probs = best_model.predict_proba(X_test)[:, 1]
        best_threshold = results[best_name]['threshold']
        best_preds = (best_probs >= best_threshold).astype(int)
    else:
        best_preds = best_model.predict(X_test)
    logging.info(f"\nClassification Report for Best Model: {best_name}")
    logging.info(classification_report(y_test, best_preds, target_names=le.classes_))

    # print accuracy for best model
    accuracy = accuracy_score(y_test, best_preds)
    print(f"Accuracy for Best Model: {accuracy:.4f}")

    
    return results[best_name], best_name, le, n_classes


# ---------------- Save ----------------
def save_artifacts(model_obj, model_name, label_encoder, n_classes, model_dir='models'):
    Path(model_dir).mkdir(exist_ok=True)
    joblib.dump(model_obj['pipeline'], Path(model_dir)/'model.pkl')
    joblib.dump(model_obj['threshold'], Path(model_dir)/'threshold.pkl')
    joblib.dump(label_encoder, Path(model_dir)/'label_encoder.pkl')
    joblib.dump(n_classes, Path(model_dir)/'n_classes.pkl')
    joblib.dump(model_name, Path(model_dir)/'model_name.pkl')
    print("✓ Model artifacts saved")


# ---------------- Main ----------------
def main(data_path, model_dir='models'):
    df = load_data(data_path)
    df = process_data(df)
    X_train, X_test, y_train, y_test = split_data(df)
    model_obj, model_name, le, n_classes = train_and_select(X_train, X_test, y_train, y_test)
    save_artifacts(model_obj, model_name, le, n_classes, model_dir=model_dir)
    return model_obj, model_name, le, n_classes


if __name__ == "__main__":
    main(save_dir / 'forecast_deviation.csv')
