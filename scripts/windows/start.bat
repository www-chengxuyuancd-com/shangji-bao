@echo off
chcp 65001 >nul
echo ==========================================
echo   商机宝 - 启动
echo ==========================================
echo.

echo [1/2] 构建应用镜像...
docker compose build
if %ERRORLEVEL% neq 0 (
    echo 构建失败，请检查 Docker Desktop 是否正在运行
    pause
    exit /b 1
)

echo [2/2] 启动所有服务...
docker compose up -d
if %ERRORLEVEL% neq 0 (
    echo 启动失败
    pause
    exit /b 1
)

echo.
echo ==========================================
echo   启动成功!
echo   前台: http://localhost:8000
echo   后台: http://localhost:8000/admin
echo   账号: admin / admin123
echo ==========================================
pause
