FROM python:3.11-slim

WORKDIR /app

# 先拷贝依赖文件，利用缓存
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 再拷贝代码
COPY . /app

# 容器对外暴露端口
EXPOSE 8000

# 启动 FastAPI（注意：用 0.0.0.0 才能被外部访问）
CMD ["python", "-m", "uvicorn", "web.app:app", "--host", "0.0.0.0", "--port", "8000"]