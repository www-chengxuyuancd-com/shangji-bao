# 商机宝 - 招标商机智能检索平台

根据特定关键词检索招标、采购等商机信息，存储原始网页并提供结构化检索。

## 技术栈

| 组件 | 技术 |
|------|------|
| Web 框架 | Flask |
| 爬虫 | Scrapy |
| 关系数据库 | PostgreSQL + Prisma ORM |
| 文档数据库 | MongoDB (存储原始网页) |
| 包管理 | uv |
| 部署 | Docker + Docker Compose |

## 快速启动

### Docker 方式（推荐）

```bash
cp .env.example .env
docker compose up -d --build
```

访问:
- 前台检索: http://localhost:5000
- 管理后台: http://localhost:5000/admin （默认账号 admin / admin123）

### 本地开发

```bash
# 安装依赖
uv sync

# 确保 PostgreSQL 和 MongoDB 已运行，然后配置环境变量
cp .env.example .env
# 编辑 .env 填入实际的数据库连接信息

# 初始化数据库
uv run prisma db push

# 启动 Web 服务
uv run flask --app src.app:create_app run --debug --port 5000

# 运行爬虫（需要先实现具体 Spider）
uv run python scripts/run_crawler.py --keyword "人工智能" --region "四川"
```

## 项目结构

```
├── docker-compose.yml      # Docker 编排
├── Dockerfile              # 应用镜像
├── pyproject.toml          # Python 项目配置 (uv)
├── scrapy.cfg              # Scrapy 配置
├── prisma/
│   └── schema.prisma       # 数据库模型定义
├── scripts/
│   └── run_crawler.py      # 爬虫启动脚本
├── src/
│   ├── config.py           # 应用配置
│   ├── app/                # Flask 应用
│   │   ├── __init__.py     # App factory
│   │   ├── routes/         # 页面路由
│   │   │   ├── frontend.py # 前台搜索
│   │   │   └── admin.py    # 后台管理
│   │   ├── api/            # API 接口
│   │   │   └── search.py
│   │   ├── templates/      # Jinja2 模板
│   │   └── static/         # 静态资源
│   ├── crawler/            # Scrapy 爬虫
│   │   ├── settings.py
│   │   ├── items.py
│   │   ├── pipelines.py    # 去重 + MongoDB + PostgreSQL
│   │   └── spiders/
│   │       └── base_spider.py
│   ├── parser/             # 解析模块（待实现）
│   │   └── base.py         # 解析器基类和注册表
│   └── db/
│       ├── prisma_client.py
│       └── mongo.py
└── tests/
```

## 数据库设计

### PostgreSQL（通过 Prisma 管理）
- `search_keywords` — 搜索关键词
- `search_regions` — 搜索地区
- `crawl_tasks` — 爬取任务
- `search_results` — 搜索结果元数据
- `visited_urls` — 已访问 URL（去重）

### MongoDB
- `raw_pages` — 原始网页 HTML + 内容 hash

## 数据库迁移

使用 Prisma 管理数据库结构变更：

```bash
# 修改 schema.prisma 后推送变更
uv run prisma db push

# 生成正式迁移文件（生产环境推荐）
uv run prisma migrate dev --name describe_your_change
```
