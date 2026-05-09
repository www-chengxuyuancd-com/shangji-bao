# Docker 离线镜像包（客户机使用）

由于客户机上 Docker Hub 镜像加速器（如 `docker-0.unsee.tech`）失效，导致拉取
基础镜像 `python:3.12-slim` 失败。本目录提供已打包好的基础镜像 tar 文件，
导入后即可跳过 Docker Hub 直接构建/启动容器。

## 文件清单

| 文件                              | 包含镜像                              | 用途                       | 大小   |
| --------------------------------- | ------------------------------------- | -------------------------- | ------ |
| `python-3.12-slim.tar`            | `python:3.12-slim`                    | web 服务的基础镜像（必装） | 142 MB |
| `base-images-pg-mongo-napcat.tar` | `postgres:16-alpine` + `mongo:7` + `mlikiowa/napcat-docker:latest` | postgres/mongo/napcat 服务 | 2.3 GB |

## 一、最小修复（只解决当前构建报错）

只需导入 `python-3.12-slim.tar` 即可：

```bash
# Windows / macOS / Linux 通用
docker load -i python-3.12-slim.tar

# 验证
docker images python:3.12-slim
# 应该看到一行 python  3.12-slim ... 142MB

# 重新构建并启动 web
docker compose up -d --build web
```

## 二、首次部署 / 完全离线环境

如果客户机是第一次部署，或想做到完全不依赖外网拉镜像，把两个 tar 都导入：

```bash
docker load -i python-3.12-slim.tar
docker load -i base-images-pg-mongo-napcat.tar

# 验证四个镜像都已存在
docker images | grep -E "python|postgres|mongo|napcat"

# 启动整套服务
docker compose up -d --build
```

## 三、常见问题

### 1. `docker load` 时报 "no space left on device"

客户机磁盘空间不足。需要 ~3GB 空闲空间存放镜像，外加 ~5GB 给容器运行。
清理：`docker system prune -a` （注意会删除未在用的镜像和容器）。

### 2. 导入后构建依然报错去拉 Docker Hub

确认两件事：

- `docker images python:3.12-slim` 能查到本地镜像
- `Dockerfile` 第一行是 `FROM python:3.12-slim`（不是 `python:3.12-slim@sha256:...`）

如果本地有镜像 docker 还要去 Hub 拉，多半是因为 `--pull` 参数被加了，
确认构建命令是 `docker compose up -d --build web`，不要带 `--pull always`。

### 3. 想关掉那个失效的镜像加速器

Docker Desktop → Settings → **Docker Engine**，找到 `registry-mirrors` 字段，
要么删掉这个字段，要么改成空数组 `"registry-mirrors": []`，再点 Apply & Restart。
关掉后即使将来要拉新镜像，也会直连 Docker Hub（需要外网）。
