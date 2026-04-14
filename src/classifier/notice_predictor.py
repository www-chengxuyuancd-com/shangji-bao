"""
公告类型预测器。

单例加载微调后的多分类模型，返回类型名称和各类别概率。
"""
import logging
import os
import json
import threading

import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "models"))
NOTICE_MODEL_DIR = os.path.join(MODEL_DIR, "bert_notice_type")

DEFAULT_LABELS = ["招标", "中标", "变更公告", "废标公告", "采购意向", "预招标", "合同", "验收公告", "其他"]


class NoticeTypePredictor:
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
        self._labels = DEFAULT_LABELS
        self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        config_path = os.path.join(NOTICE_MODEL_DIR, "config.json")
        if os.path.exists(config_path):
            try:
                self._tokenizer = AutoTokenizer.from_pretrained(NOTICE_MODEL_DIR)
                self._model = AutoModelForSequenceClassification.from_pretrained(NOTICE_MODEL_DIR)
                self._model.to(self._device)
                self._model.eval()

                meta_path = os.path.join(NOTICE_MODEL_DIR, "meta.json")
                if os.path.exists(meta_path):
                    with open(meta_path) as f:
                        meta = json.load(f)
                    self._labels = meta.get("labels", DEFAULT_LABELS)

                logger.info("Notice type model loaded from %s", NOTICE_MODEL_DIR)
            except Exception as e:
                logger.warning("Failed to load notice type model: %s", e)

    @property
    def available(self) -> bool:
        return self._model is not None

    def predict(self, text: str, title: str = "") -> dict | None:
        """
        预测公告类型。
        返回 {"type": "招标", "confidence": 0.95, "probabilities": {"招标": 0.95, ...}}
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
                probs = torch.softmax(outputs.logits, dim=1)[0]

            prob_dict = {self._labels[i]: round(probs[i].item(), 4) for i in range(len(self._labels))}
            best_idx = torch.argmax(probs).item()

            return {
                "type": self._labels[best_idx],
                "confidence": round(probs[best_idx].item(), 4),
                "probabilities": prob_dict,
            }
        except Exception as e:
            logger.warning("Notice type predict error: %s", e)
            return None
