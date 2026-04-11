@echo off
chcp 65001 >nul
echo ==========================================
echo   商机宝 - 首次安装（完整流程）
echo ==========================================
echo.
echo 此脚本会依次执行:
echo   1. 构建并启动所有服务
echo   2. 导入数据库数据
echo.

:: 检查 data 目录
if not exist "data\postgres_dump.sql" (
    echo 警告: data 目录不存在或数据文件缺失
    echo 如果需要导入数据，请先将 data-*.tar.gz 解压到项目根目录
    echo.
    set /p CONTINUE="是否继续（不导入数据）？(Y/N): "
    if /i not "%CONTINUE%"=="Y" exit /b 0
    set SKIP_DATA=1
) else (
    set SKIP_DATA=0
)

echo [1/3] 构建应用镜像（首次约5-10分钟）...
docker compose build
if %ERRORLEVEL% neq 0 (
    echo 构建失败，请确认:
    echo   1. Docker Desktop 已安装并正在运行
    echo   2. 网络连接正常
    pause
    exit /b 1
)

echo [2/3] 启动所有服务...
docker compose up -d
if %ERRORLEVEL% neq 0 (
    echo 启动失败
    pause
    exit /b 1
)

echo 等待数据库就绪...
timeout /t 15 /nobreak >nul

if "%SKIP_DATA%"=="0" (
    echo [3/3] 导入数据...
    echo   导入 PostgreSQL...
    docker compose exec -T postgres psql -U postgres -d shangjibao < data\postgres_dump.sql
    echo   导入 MongoDB...
    docker compose exec -T mongodb mongorestore --username mongodb --password mongodb --authenticationDatabase admin --db shangjibao --drop --archive < data\mongo_dump.archive
    echo   数据导入完成
) else (
    echo [3/3] 跳过数据导入
)

echo.
echo ==========================================
echo   安装完成!
echo   前台: http://localhost:8000
echo   后台: http://localhost:8000/admin
echo   账号: admin / admin123
echo.
echo   日常操作:
echo     启动: scripts\windows\start.bat
echo     停止: scripts\windows\stop.bat
echo     更新代码: scripts\windows\update.bat
echo     重新导入数据: scripts\windows\import_data.bat
echo ==========================================
pause
