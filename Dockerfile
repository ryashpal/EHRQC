# Refer: https://medium.com/swlh/dockerize-your-python-command-line-program-6a273f5c5544

FROM python:3.9-slim
RUN useradd --create-home --shell /bin/bash app_user
WORKDIR /home/app_user
RUN apt-get update && apt-get install
COPY requirements.txt ./
# RUN apt-get install libpq-dev
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir /home/app_user/data
RUN chown -R app_user.app_user /home/app_user/data
USER app_user
COPY . .
RUN python -m venv .venv
RUN .venv/bin/pip install --no-cache-dir -r requirements.txt
CMD ["bash"]
