@echo off
chcp 65001 >nul
echo ==========================================
echo   商机宝 - 更新代码
echo ==========================================
echo.

echo [1/3] 拉取最新代码...
git pull
if %ERRORLEVEL% neq 0 (
    echo Git 拉取失败，请检查网络
    pause
    exit /b 1
)

echo [2/3] 重新构建镜像...
docker compose build web
if %ERRORLEVEL% neq 0 (
    echo 构建失败
    pause
    exit /b 1
)

echo [3/3] 重启应用...
docker compose up -d web
if %ERRORLEVEL% neq 0 (
    echo 重启失败
    pause
    exit /b 1
)

echo.
echo ==========================================
echo   更新完成!
echo   访问: http://localhost:8000
echo ==========================================
pause
