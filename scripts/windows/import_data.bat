@echo off
chcp 65001 >nul
echo ==========================================
echo   商机宝 - 导入数据
echo ==========================================
echo.

:: 检查 data 目录是否存在
if not exist "data\postgres_dump.sql" (
    echo 错误: 找不到 data\postgres_dump.sql
    echo 请先将 data-*.tar.gz 解压到项目根目录
    echo 解压后应有 data\postgres_dump.sql 和 data\mongo_dump.archive
    pause
    exit /b 1
)

echo [1/2] 导入 PostgreSQL 数据...
docker compose exec -T postgres psql -U postgres -d shangjibao < data\postgres_dump.sql
if %ERRORLEVEL% neq 0 (
    echo PostgreSQL 导入失败
    pause
    exit /b 1
)
echo   完成

echo [2/2] 导入 MongoDB 数据...
docker compose exec -T mongodb mongorestore --username mongodb --password mongodb --authenticationDatabase admin --db shangjibao --drop --archive < data\mongo_dump.archive
if %ERRORLEVEL% neq 0 (
    echo MongoDB 导入失败
    pause
    exit /b 1
)
echo   完成

echo.
echo ==========================================
echo   数据导入完成!
echo ==========================================
pause
