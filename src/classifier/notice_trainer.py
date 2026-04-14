"""
公告类型多分类模型训练。

使用 chinese-roberta-wwm-ext 微调，从 ParsedResult 的已有 noticeType 数据训练。
支持：招标、中标、变更公告、废标公告、采购意向、预招标、合同、验收公告 等类别。
"""
import logging
import os
import json
import threading

os.environ["SAFETENSORS_FAST_GPU"] = "0"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"

import torch
from torch.utils.data import Dataset, DataLoader, random_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "models"))
NOTICE_MODEL_DIR = os.path.join(MODEL_DIR, "bert_notice_type")
BASE_MODEL = "hfl/chinese-roberta-wwm-ext"
HF_MIRROR = os.getenv("HF_MIRROR", "https://hf-mirror.com")

_training_status = {"running": False, "progress": "", "result": None}

NOTICE_LABELS = ["招标", "中标", "变更公告", "废标公告", "采购意向", "预招标", "合同", "验收公告", "其他"]


class NoticeTypeDataset(Dataset):
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


def _prepare_notice_data(prisma):
    """从 ParsedResult 读取有 noticeType 的数据作为训练集。"""
    items = prisma.parsedresult.find_many(
        where={"noticeType": {"not": None}},
        take=5000,
    )
    texts, labels = [], []
    label2idx = {label: i for i, label in enumerate(NOTICE_LABELS)}

    for item in items:
        nt = item.noticeType
        if nt not in label2idx:
            nt = "其他"
        title = (item.title or "").strip()
        summary = (item.summary or "").strip()[:500]
        if not title:
            continue
        text = f"{title} [SEP] {summary}" if summary else title
        texts.append(text)
        labels.append(label2idx[nt])

    return texts, labels


def get_training_status():
    return dict(_training_status)


def train_notice_model(prisma=None, epochs=5, batch_size=16, lr=2e-5) -> dict:
    """微调 BERT 公告类型多分类模型。"""
    own_prisma = prisma is None
    if own_prisma:
        from prisma import Prisma
        prisma = Prisma()
        prisma.connect()

    try:
        _training_status["progress"] = "准备数据..."
        texts, labels = _prepare_notice_data(prisma)
        total = len(texts)

        if total < 20:
            return {"success": False, "error": f"有标注类型的数据仅 {total} 条，至少需要 20 条"}

        label_dist = {}
        for l in labels:
            name = NOTICE_LABELS[l]
            label_dist[name] = label_dist.get(name, 0) + 1

        _training_status["progress"] = "加载预训练模型..."
        os.environ["HF_ENDPOINT"] = HF_MIRROR
        tokenizer = AutoTokenizer.from_pretrained(
            BASE_MODEL,
            use_safetensors=False,
        )
        model = AutoModelForSequenceClassification.from_pretrained(
            BASE_MODEL,
            num_labels=len(NOTICE_LABELS),
            use_safetensors=False,
            ignore_mismatched_sizes=True,
        )
        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        model.to(device)

        _training_status["progress"] = "构建数据集..."
        dataset = NoticeTypeDataset(texts, labels, tokenizer)
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
                outputs.loss.backward()
                optimizer.step()
                total_loss += outputs.loss.item()

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
            logger.info("Notice Epoch %d/%d - loss: %.4f, val_acc: %.4f", epoch + 1, epochs, avg_loss, acc)
            if acc >= best_acc:
                best_acc = acc

        _training_status["progress"] = "保存模型..."
        os.makedirs(NOTICE_MODEL_DIR, exist_ok=True)
        model.save_pretrained(NOTICE_MODEL_DIR)
        tokenizer.save_pretrained(NOTICE_MODEL_DIR)

        meta = {
            "base_model": BASE_MODEL,
            "labels": NOTICE_LABELS,
            "samples": total,
            "label_distribution": label_dist,
            "epochs": epochs,
            "val_accuracy": round(best_acc, 4),
        }
        with open(os.path.join(NOTICE_MODEL_DIR, "meta.json"), "w") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        from src.classifier.notice_predictor import NoticeTypePredictor
        NoticeTypePredictor.reload()

        return {"success": True, **meta}

    except Exception as e:
        logger.error("Notice type training failed: %s", e)
        return {"success": False, "error": str(e)}
    finally:
        if own_prisma:
            prisma.disconnect()


def start_training_async(db_url: str, epochs=5, batch_size=16, lr=2e-5):
    """在后台线程启动训练。"""
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
            result = train_notice_model(prisma, epochs=epochs, batch_size=batch_size, lr=lr)
            _training_status["result"] = result
        except Exception as e:
            _training_status["result"] = {"success": False, "error": str(e)}
        finally:
            prisma.disconnect()
            _training_status["running"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return True
