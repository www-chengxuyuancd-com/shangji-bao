@echo off
chcp 65001 >nul
echo 正在停止商机宝...
docker compose down
echo 已停止。
pause
