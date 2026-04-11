"""
FastText 模型训练。

从 LabeledSample 读取已标注数据，jieba 分词后训练 FastText 分类模型。
"""
import logging
import os
import tempfile

logger = logging.getLogger(__name__)

MODEL_DIR = os.getenv("MODEL_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data", "models"))
MODEL_PATH = os.path.join(MODEL_DIR, "relevance_model.bin")


def prepare_training_data(prisma) -> str:
    """
    将已标注样本导出为 FastText 训练格式。
    返回临时文件路径。
    """
    import jieba

    samples = prisma.labeledsample.find_many(
        where={"label": {"not": None}},
    )

    fd, path = tempfile.mkstemp(suffix=".txt", prefix="ft_train_")
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for s in samples:
            text = (s.content or s.title or "")[:2000]
            if not text.strip():
                continue
            label = "__label__relevant" if s.label == 1 else "__label__irrelevant"
            words = " ".join(jieba.cut(text))
            f.write(f"{label} {words}\n")

    return path


def train_model(prisma=None) -> dict:
    """
    训练 FastText 分类模型。
    返回训练摘要 dict。
    """
    import fasttext

    own_prisma = prisma is None
    if own_prisma:
        from prisma import Prisma
        prisma = Prisma()
        prisma.connect()

    try:
        labeled_count = prisma.labeledsample.count(where={"label": {"not": None}})
        relevant_count = prisma.labeledsample.count(where={"label": 1})
        irrelevant_count = prisma.labeledsample.count(where={"label": 0})

        if labeled_count < 10:
            return {"success": False, "error": f"已标注样本仅 {labeled_count} 条，建议至少 50 条"}

        train_file = prepare_training_data(prisma)

        os.makedirs(MODEL_DIR, exist_ok=True)

        model = fasttext.train_supervised(
            input=train_file,
            epoch=25,
            lr=0.5,
            wordNgrams=2,
            dim=100,
            loss="softmax",
            minCount=2,
        )

        result = model.test(train_file)
        precision = round(result[1], 4)
        recall = round(result[2], 4)

        model.save_model(MODEL_PATH)

        os.unlink(train_file)

        from src.classifier.predictor import RelevancePredictor
        RelevancePredictor.reload()

        summary = {
            "success": True,
            "samples": labeled_count,
            "relevant": relevant_count,
            "irrelevant": irrelevant_count,
            "precision": precision,
            "recall": recall,
            "model_path": MODEL_PATH,
        }
        logger.info("Model trained: %s", summary)
        return summary

    finally:
        if own_prisma:
            prisma.disconnect()
