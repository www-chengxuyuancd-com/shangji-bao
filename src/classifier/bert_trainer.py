"""
BERT 相关性分类模型训练。

使用 chinese-roberta-wwm-ext 微调，从 LabeledSample 读取数据，
输出 0~1 连续分数，支持自定义阈值。
"""
import logging
import os
import json
import threading
import time

os.environ["SAFETENSORS_FAST_GPU"] = "0"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"

import torch
from torch.utils.data import Dataset, DataLoader, random_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification

# 禁用 transformers 自动 safetensors 转换检查（hf-mirror 不支持 discussions API）
try:
    import transformers.safetensors_conversion as _sc
    _sc.auto_conversion = lambda *a, **kw: None
except Exception:
    pass

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "models"))
BERT_MODEL_DIR = os.path.join(MODEL_DIR, "bert_relevance")
BASE_MODEL = "hfl/chinese-roberta-wwm-ext"
HF_MIRROR = os.getenv("HF_MIRROR", "https://hf-mirror.com")

_training_status = {"running": False, "progress": "", "result": None}


class RelevanceDataset(Dataset):
    def __init__(self, texts, labels, tokenizer, max_len=256):
        self.texts = texts
        self.labels = labels
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        enc = self.tokenizer(
            self.texts[idx],
            truncation=True,
            padding="max_length",
            max_length=self.max_len,
            return_tensors="pt",
        )
        return {
            "input_ids": enc["input_ids"].squeeze(0),
            "attention_mask": enc["attention_mask"].squeeze(0),
            "labels": torch.tensor(self.labels[idx], dtype=torch.long),
        }


def _prepare_data(prisma):
    """从 LabeledSample 读取已标注数据，返回 (texts, labels) 列表。"""
    samples = prisma.labeledsample.find_many(where={"label": {"not": None}})
    texts, labels = [], []
    for s in samples:
        title = (s.title or "").strip()
        body = (s.content or "").strip()[:500]
        if not title and not body:
            continue
        text = f"{title} [SEP] {body}" if body else title
        texts.append(text)
        labels.append(s.label)
    return texts, labels


def get_training_status():
    return dict(_training_status)


def train_bert_model(prisma=None, epochs=5, batch_size=16, lr=2e-5) -> dict:
    """
    微调 BERT 相关性二分类模型。
    返回训练摘要 dict。
    """
    own_prisma = prisma is None
    if own_prisma:
        from prisma import Prisma
        prisma = Prisma()
        prisma.connect()

    try:
        _training_status["progress"] = "准备数据..."
        texts, labels = _prepare_data(prisma)
        total = len(texts)
        relevant = sum(labels)
        irrelevant = total - relevant

        if total < 20:
            return {"success": False, "error": f"已标注样本仅 {total} 条，至少需要 20 条"}

        _training_status["progress"] = "加载预训练模型..."
        os.environ["HF_ENDPOINT"] = HF_MIRROR
        tokenizer = AutoTokenizer.from_pretrained(
            BASE_MODEL,
            use_safetensors=False,
        )
        model = AutoModelForSequenceClassification.from_pretrained(
            BASE_MODEL,
            num_labels=2,
            use_safetensors=False,
            ignore_mismatched_sizes=True,
        )
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device)

        _training_status["progress"] = "构建数据集..."
        dataset = RelevanceDataset(texts, labels, tokenizer)

        val_size = max(int(total * 0.15), 2)
        train_size = total - val_size
        train_ds, val_ds = random_split(dataset, [train_size, val_size])

        train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)
        val_loader = DataLoader(val_ds, batch_size=batch_size)

        optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=0.01)

        best_acc = 0.0
        for epoch in range(epochs):
            _training_status["progress"] = f"训练 Epoch {epoch + 1}/{epochs}..."
            model.train()
            total_loss = 0
            for batch in train_loader:
                optimizer.zero_grad()
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)
                batch_labels = batch["labels"].to(device)
                outputs = model(input_ids=input_ids, attention_mask=attention_mask, labels=batch_labels)
                loss = outputs.loss
                loss.backward()
                optimizer.step()
                total_loss += loss.item()

            model.eval()
            correct, total_val = 0, 0
            with torch.no_grad():
                for batch in val_loader:
                    input_ids = batch["input_ids"].to(device)
                    attention_mask = batch["attention_mask"].to(device)
                    batch_labels = batch["labels"].to(device)
                    outputs = model(input_ids=input_ids, attention_mask=attention_mask)
                    preds = torch.argmax(outputs.logits, dim=1)
                    correct += (preds == batch_labels).sum().item()
                    total_val += batch_labels.size(0)

            acc = correct / total_val if total_val > 0 else 0
            avg_loss = total_loss / len(train_loader)
            _training_status["progress"] = f"Epoch {epoch + 1}/{epochs} 完成 - loss: {avg_loss:.4f}, acc: {acc:.4f}"
            logger.info("Epoch %d/%d - loss: %.4f, val_acc: %.4f", epoch + 1, epochs, avg_loss, acc)
            if acc >= best_acc:
                best_acc = acc

        _training_status["progress"] = "保存模型..."
        os.makedirs(BERT_MODEL_DIR, exist_ok=True)
        model.save_pretrained(BERT_MODEL_DIR)
        tokenizer.save_pretrained(BERT_MODEL_DIR)

        meta = {
            "base_model": BASE_MODEL,
            "samples": total,
            "relevant": relevant,
            "irrelevant": irrelevant,
            "epochs": epochs,
            "val_accuracy": round(best_acc, 4),
        }
        with open(os.path.join(BERT_MODEL_DIR, "meta.json"), "w") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        from src.classifier.bert_predictor import BertRelevancePredictor
        BertRelevancePredictor.reload()

        return {"success": True, **meta}

    except Exception as e:
        logger.error("BERT training failed: %s", e)
        return {"success": False, "error": str(e)}
    finally:
        if own_prisma:
            prisma.disconnect()


def start_training_async(db_url: str, epochs=5, batch_size=16, lr=2e-5):
    """在后台线程启动训练，避免阻塞 gunicorn worker。"""
    if _training_status["running"]:
        return False

    _training_status["running"] = True
    _training_status["progress"] = "初始化..."
    _training_status["result"] = None

    def _run():
        from prisma import Prisma
        prisma = Prisma()
        prisma.connect()
        try:
            result = train_bert_model(prisma, epochs=epochs, batch_size=batch_size, lr=lr)
            _training_status["result"] = result
        except Exception as e:
            _training_status["result"] = {"success": False, "error": str(e)}
        finally:
            prisma.disconnect()
            _training_status["running"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return True
