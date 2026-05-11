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
from torch.utils.data import Dataset, DataLoader, Subset, WeightedRandomSampler
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


def _stratified_split(labels: list[int], val_ratio: float = 0.15, seed: int = 42):
    """按标签分层切分索引，保证训练/验证集类别比例一致。"""
    import random
    rnd = random.Random(seed)

    by_class: dict[int, list[int]] = {}
    for idx, lbl in enumerate(labels):
        by_class.setdefault(int(lbl), []).append(idx)

    train_idx, val_idx = [], []
    for lbl, idxs in by_class.items():
        rnd.shuffle(idxs)
        v = max(1, int(len(idxs) * val_ratio))
        val_idx.extend(idxs[:v])
        train_idx.extend(idxs[v:])

    rnd.shuffle(train_idx)
    rnd.shuffle(val_idx)
    return train_idx, val_idx


def _compute_metrics(preds: list[int], targets: list[int]) -> dict:
    """二分类指标：accuracy / precision / recall / f1（针对正类 label=1）。"""
    tp = sum(1 for p, t in zip(preds, targets) if p == 1 and t == 1)
    fp = sum(1 for p, t in zip(preds, targets) if p == 1 and t == 0)
    fn = sum(1 for p, t in zip(preds, targets) if p == 0 and t == 1)
    tn = sum(1 for p, t in zip(preds, targets) if p == 0 and t == 0)
    total = tp + fp + fn + tn
    acc = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
    return {
        "accuracy": acc,
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "tp": tp, "fp": fp, "fn": fn, "tn": tn,
    }


def train_bert_model(
    prisma=None,
    epochs=5,
    batch_size=16,
    lr=2e-5,
    balance_strategy: str | None = None,
) -> dict:
    """
    微调 BERT 相关性二分类模型，处理类别不平衡。

    balance_strategy:
        None / "auto"  - 自动：正负比 >3:1 时启用 weighted_sampler + class_weight
        "none"         - 关闭，按原数据分布训练
        "sampler"      - 仅启用 WeightedRandomSampler
        "weight"       - 仅启用 CrossEntropyLoss(class_weight)
        "both"         - 同时启用（强不平衡推荐）
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
        if relevant < 5 or irrelevant < 5:
            return {
                "success": False,
                "error": f"两类样本至少各 5 条（当前 相关={relevant}, 不相关={irrelevant}）",
            }

        imbalance_ratio = max(relevant, irrelevant) / max(min(relevant, irrelevant), 1)

        # 自动决策不平衡策略
        if balance_strategy in (None, "auto"):
            balance_strategy = "both" if imbalance_ratio > 3 else "none"

        use_sampler = balance_strategy in ("sampler", "both")
        use_class_weight = balance_strategy in ("weight", "both")

        logger.info(
            "BERT training: total=%d, relevant=%d, irrelevant=%d, imbalance_ratio=%.2f:1, strategy=%s",
            total, relevant, irrelevant, imbalance_ratio, balance_strategy,
        )

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

        train_idx, val_idx = _stratified_split(labels, val_ratio=0.15, seed=42)
        train_ds = Subset(dataset, train_idx)
        val_ds = Subset(dataset, val_idx)

        train_labels = [labels[i] for i in train_idx]
        val_relevant = sum(labels[i] for i in val_idx)
        val_irrelevant = len(val_idx) - val_relevant

        logger.info(
            "Stratified split: train=%d (rel=%d, irrel=%d), val=%d (rel=%d, irrel=%d)",
            len(train_idx), sum(train_labels), len(train_labels) - sum(train_labels),
            len(val_idx), val_relevant, val_irrelevant,
        )

        if use_sampler:
            # 每条样本的权重 = 1 / 该类样本数；按 batch 抽样后正负比约 1:1
            n_rel = max(sum(train_labels), 1)
            n_irrel = max(len(train_labels) - n_rel, 1)
            class_weights_for_sampler = {1: 1.0 / n_rel, 0: 1.0 / n_irrel}
            sample_weights = [class_weights_for_sampler[lbl] for lbl in train_labels]
            sampler = WeightedRandomSampler(
                weights=sample_weights,
                num_samples=len(sample_weights),
                replacement=True,
            )
            train_loader = DataLoader(
                train_ds, batch_size=batch_size, sampler=sampler,
            )
        else:
            train_loader = DataLoader(train_ds, batch_size=batch_size, shuffle=True)

        val_loader = DataLoader(val_ds, batch_size=batch_size)

        if use_class_weight:
            # 类别加权交叉熵：少数类的样本误分类代价更高
            n_rel = max(sum(train_labels), 1)
            n_irrel = max(len(train_labels) - n_rel, 1)
            n_total = n_rel + n_irrel
            # 标准 inverse-frequency 加权（sklearn "balanced" 风格）
            class_weights = torch.tensor(
                [n_total / (2.0 * n_irrel), n_total / (2.0 * n_rel)],
                dtype=torch.float,
                device=device,
            )
            criterion = torch.nn.CrossEntropyLoss(weight=class_weights)
            logger.info(
                "CrossEntropyLoss class_weight = [irrelevant=%.3f, relevant=%.3f]",
                class_weights[0].item(), class_weights[1].item(),
            )
        else:
            criterion = torch.nn.CrossEntropyLoss()

        optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=0.01)

        best_f1 = -1.0
        best_metrics: dict | None = None
        history: list[dict] = []

        for epoch in range(epochs):
            _training_status["progress"] = f"训练 Epoch {epoch + 1}/{epochs}..."
            t_epoch = time.time()
            model.train()
            total_loss = 0.0
            n_batches = len(train_loader)
            for bi, batch in enumerate(train_loader):
                optimizer.zero_grad()
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)
                batch_labels = batch["labels"].to(device)
                outputs = model(
                    input_ids=input_ids, attention_mask=attention_mask,
                )
                loss = criterion(outputs.logits, batch_labels)
                loss.backward()
                optimizer.step()
                total_loss += loss.item()

                if n_batches >= 20 and (bi + 1) % max(1, n_batches // 10) == 0:
                    _training_status["progress"] = (
                        f"Epoch {epoch + 1}/{epochs} 训练中 "
                        f"[{bi + 1}/{n_batches}] loss={loss.item():.4f}"
                    )

            model.eval()
            all_preds: list[int] = []
            all_targets: list[int] = []
            with torch.no_grad():
                for batch in val_loader:
                    input_ids = batch["input_ids"].to(device)
                    attention_mask = batch["attention_mask"].to(device)
                    batch_labels = batch["labels"].to(device)
                    outputs = model(
                        input_ids=input_ids, attention_mask=attention_mask,
                    )
                    preds = torch.argmax(outputs.logits, dim=1)
                    all_preds.extend(preds.cpu().tolist())
                    all_targets.extend(batch_labels.cpu().tolist())

            metrics = _compute_metrics(all_preds, all_targets)
            avg_loss = total_loss / max(n_batches, 1)
            elapsed = time.time() - t_epoch

            summary = (
                f"Epoch {epoch + 1}/{epochs} done "
                f"loss={avg_loss:.4f} acc={metrics['accuracy']:.4f} "
                f"P={metrics['precision']:.4f} R={metrics['recall']:.4f} "
                f"F1={metrics['f1']:.4f} "
                f"(TP={metrics['tp']}/FP={metrics['fp']}/"
                f"FN={metrics['fn']}/TN={metrics['tn']}) "
                f"[{elapsed:.1f}s]"
            )
            logger.info(summary)
            _training_status["progress"] = (
                f"Epoch {epoch + 1}/{epochs} loss={avg_loss:.4f} "
                f"P={metrics['precision']:.3f} R={metrics['recall']:.3f} "
                f"F1={metrics['f1']:.3f}"
            )

            history.append({
                "epoch": epoch + 1,
                "loss": round(avg_loss, 4),
                **{k: (round(v, 4) if isinstance(v, float) else v) for k, v in metrics.items()},
                "elapsed_sec": round(elapsed, 1),
            })

            # 以 F1 作为最优模型选择标准（不平衡数据下比 accuracy 更可靠）
            if metrics["f1"] > best_f1:
                best_f1 = metrics["f1"]
                best_metrics = metrics

        _training_status["progress"] = "保存模型..."
        os.makedirs(BERT_MODEL_DIR, exist_ok=True)
        model.save_pretrained(BERT_MODEL_DIR)
        tokenizer.save_pretrained(BERT_MODEL_DIR)

        meta = {
            "base_model": BASE_MODEL,
            "samples": total,
            "relevant": relevant,
            "irrelevant": irrelevant,
            "imbalance_ratio": round(imbalance_ratio, 2),
            "balance_strategy": balance_strategy,
            "epochs": epochs,
            "val_accuracy": round(best_metrics["accuracy"], 4) if best_metrics else 0.0,
            "val_precision": round(best_metrics["precision"], 4) if best_metrics else 0.0,
            "val_recall": round(best_metrics["recall"], 4) if best_metrics else 0.0,
            "val_f1": round(best_metrics["f1"], 4) if best_metrics else 0.0,
            "history": history,
        }
        with open(os.path.join(BERT_MODEL_DIR, "meta.json"), "w") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2)

        from src.classifier.bert_predictor import BertRelevancePredictor
        BertRelevancePredictor.reload()

        return {"success": True, **meta}

    except Exception as e:
        logger.error("BERT training failed: %s", e, exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        if own_prisma:
            prisma.disconnect()


def start_training_async(
    db_url: str,
    epochs=5,
    batch_size=16,
    lr=2e-5,
    balance_strategy: str | None = None,
):
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
            result = train_bert_model(
                prisma,
                epochs=epochs,
                batch_size=batch_size,
                lr=lr,
                balance_strategy=balance_strategy,
            )
            _training_status["result"] = result
        except Exception as e:
            _training_status["result"] = {"success": False, "error": str(e)}
        finally:
            prisma.disconnect()
            _training_status["running"] = False

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return True
