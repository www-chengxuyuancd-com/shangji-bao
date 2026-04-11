#!/bin/bash
# 导出数据库数据 + 模型文件（代码用 Git 管理，数据单独传输）
# 用法: bash scripts/export_data.sh
# 输出: data/ 目录 和 data-YYYYMMDD.tar.gz 压缩包

set -e

DATA_DIR="data"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
ARCHIVE_NAME="data-${TIMESTAMP}.tar.gz"

echo "=========================================="
echo "  商机宝 - 导出数据"
echo "=========================================="

mkdir -p "$DATA_DIR/models"

echo "[1/3] 导出 PostgreSQL 数据..."
docker compose exec -T postgres pg_dump -U postgres -d shangjibao \
    --clean --if-exists > "$DATA_DIR/postgres_dump.sql"
echo "  -> $(du -h "$DATA_DIR/postgres_dump.sql" | cut -f1)"

echo "[2/3] 导出 MongoDB 数据..."
docker compose exec -T mongodb mongodump \
    --username mongodb --password mongodb \
    --authenticationDatabase admin \
    --db shangjibao --archive > "$DATA_DIR/mongo_dump.archive"
echo "  -> $(du -h "$DATA_DIR/mongo_dump.archive" | cut -f1)"

echo "[3/3] 导出模型文件..."
docker compose cp web:/app/data/models/. "$DATA_DIR/models/" 2>/dev/null || echo "  (无模型文件，跳过)"

echo ""
echo "打包压缩..."
tar -czf "$ARCHIVE_NAME" "$DATA_DIR"

echo ""
echo "=========================================="
echo "  导出完成!"
echo "  目录: $DATA_DIR/"
echo "  压缩包: $ARCHIVE_NAME ($(du -h "$ARCHIVE_NAME" | cut -f1))"
echo ""
echo "  将 $ARCHIVE_NAME 发给客户，放到项目根目录解压即可"
echo "=========================================="
