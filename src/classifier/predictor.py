"""
FastText 模型预测。

单例模式加载模型，提供 predict / predict_batch 接口。
模型不存在时降级返回 None。
"""
import logging
import os
import threading

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "models"))
MODEL_PATH = os.path.join(MODEL_DIR, "relevance_model.bin")


class RelevancePredictor:
    _instance = None
    _lock = threading.Lock()
    _model = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
        return cls._instance

    @classmethod
    def reload(cls):
        with cls._lock:
            cls._instance = cls()

    def __init__(self):
        self._model = None
        if os.path.exists(MODEL_PATH):
            try:
                import fasttext
                self._model = fasttext.load_model(MODEL_PATH)
                logger.info("FastText model loaded from %s", MODEL_PATH)
            except Exception as e:
                logger.warning("Failed to load FastText model: %s", e)

    @property
    def available(self) -> bool:
        return self._model is not None

    def predict(self, text: str) -> dict | None:
        if not self._model:
            return None
        try:
            import jieba
            words = " ".join(jieba.cut(text[:2000]))
            labels, probs = self._model.predict(words)
            label_str = labels[0].replace("__label__", "")
            confidence = float(probs[0])
            return {"label": label_str, "confidence": round(confidence, 4)}
        except Exception as e:
            logger.warning("Predict error: %s", e)
            return None

    def predict_batch(self, texts: list[str]) -> list[dict | None]:
        return [self.predict(t) for t in texts]
