"""
BERT 相关性预测器。

单例加载微调后的模型，输出 0~1 连续相关性分数。
模型不存在时降级返回 None。
"""
import logging
import os
import threading

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
        self._device = None

        config_path = os.path.join(BERT_MODEL_DIR, "config.json")
        if os.path.exists(config_path):
            try:
                import torch
                from transformers import AutoTokenizer, AutoModelForSequenceClassification

                self._device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
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
        results = self.predict_batch([text], [title])
        return results[0] if results else None

    def predict_batch(
        self,
        texts: list[str],
        titles: list[str] | None = None,
        batch_size: int = 32,
    ) -> list[dict | None]:
        """
        批量预测相关性。返回与输入等长的列表，每项为 {"score", "label"} 或 None。

        相比逐条 predict，批量推理可显著降低 tokenizer / forward 调用开销，
        在 CPU 上典型加速 5~10x，GPU 上 10~50x。
        """
        if not self._model:
            return [None] * len(texts)
        if not texts:
            return []
        if titles is None:
            titles = [""] * len(texts)
        elif len(titles) != len(texts):
            titles = (list(titles) + [""] * len(texts))[: len(texts)]

        inputs: list[str] = []
        for text, title in zip(texts, titles):
            text = text or ""
            title = title or ""
            inputs.append(f"{title} [SEP] {text[:500]}" if title else text[:500])

        try:
            import torch
        except Exception as e:
            logger.warning("BERT predict_batch import torch failed: %s", e)
            return [None] * len(texts)

        results: list[dict | None] = []
        for start in range(0, len(inputs), batch_size):
            chunk = inputs[start : start + batch_size]
            try:
                enc = self._tokenizer(
                    chunk,
                    truncation=True,
                    padding=True,
                    max_length=256,
                    return_tensors="pt",
                )
                input_ids = enc["input_ids"].to(self._device)
                attention_mask = enc["attention_mask"].to(self._device)

                with torch.no_grad():
                    outputs = self._model(
                        input_ids=input_ids, attention_mask=attention_mask
                    )
                    probs = torch.softmax(outputs.logits, dim=1)
                    relevant_probs = probs[:, 1].detach().cpu().tolist()

                for p in relevant_probs:
                    results.append(
                        {
                            "score": round(float(p), 4),
                            "label": "relevant" if p >= 0.5 else "irrelevant",
                        }
                    )
            except Exception as e:
                logger.warning("BERT predict_batch error (batch %d): %s", start, e)
                results.extend([None] * len(chunk))

        return results
