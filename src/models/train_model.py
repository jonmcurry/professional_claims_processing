import csv
from pathlib import Path

import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from ..config.config import load_config
from .features import extract_features


def _load_data(path: Path):
    X = []
    y = []
    with path.open() as f:
        reader = csv.DictReader(f)
        for row in reader:
            label = int(row.pop("label"))
            features = extract_features(row)
            vector = [features[k] for k in sorted(features)]
            X.append(vector)
            y.append(label)
    return X, y


def train_and_save_model(
    data_path: str | None = None, model_path: str | None = None
) -> None:
    cfg = load_config()
    if data_path is None:
        data_path = "data/training_data.csv"
    if model_path is None:
        model_path = cfg.model.path
    X, y = _load_data(Path(data_path))
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train, y_train)
    preds = clf.predict(X_test)
    acc = accuracy_score(y_test, preds)
    joblib.dump(clf, model_path)
    print(f"Model saved to {model_path} with accuracy {acc:.4f}")


if __name__ == "__main__":
    train_and_save_model()
