"""
BERT 相关性预测器。

单例加载微调后的模型，输出 0~1 连续相关性分数。
模型不存在时降级返回 None。
"""
import logging
import os
import threading

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "models"))
BERT_MODEL_DIR = os.path.join(MODEL_DIR, "bert_relevance")


class BertRelevancePredictor:
    _instance = None
    _lock = threading.Lock()

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
        self._tokenizer = None
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        config_path = os.path.join(BERT_MODEL_DIR, "config.json")
        if os.path.exists(config_path):
            try:
                self._tokenizer = AutoTokenizer.from_pretrained(BERT_MODEL_DIR)
                self._model = AutoModelForSequenceClassification.from_pretrained(BERT_MODEL_DIR)
                self._model.to(self._device)
                self._model.eval()
                logger.info("BERT relevance model loaded from %s", BERT_MODEL_DIR)
            except Exception as e:
                logger.warning("Failed to load BERT model: %s", e)

    @property
    def available(self) -> bool:
        return self._model is not None

    def predict(self, text: str, title: str = "") -> dict | None:
        """
        预测相关性，返回 {"score": 0.0~1.0, "label": "relevant"/"irrelevant"}。
        score 是相关的概率值，可用于阈值判断。
        """
        if not self._model:
            return None
        try:
            input_text = f"{title} [SEP] {text[:500]}" if title else text[:500]
            enc = self._tokenizer(
                input_text,
                truncation=True,
                padding="max_length",
                max_length=256,
                return_tensors="pt",
            )
            input_ids = enc["input_ids"].to(self._device)
            attention_mask = enc["attention_mask"].to(self._device)

            with torch.no_grad():
                outputs = self._model(input_ids=input_ids, attention_mask=attention_mask)
                probs = torch.softmax(outputs.logits, dim=1)
                relevant_prob = probs[0][1].item()

            return {
                "score": round(relevant_prob, 4),
                "label": "relevant" if relevant_prob >= 0.5 else "irrelevant",
            }
        except Exception as e:
            logger.warning("BERT predict error: %s", e)
            return None
