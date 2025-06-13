from typing import Any, Dict
import joblib


class FilterModel:
    def __init__(self, path: str):
        self.path = path
        self.model = joblib.load(path)

    def predict(self, features: Dict[str, Any]) -> int:
        vector = [features[k] for k in sorted(features)]
        return int(self.model.predict([vector])[0])
