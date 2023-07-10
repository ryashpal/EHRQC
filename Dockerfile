# Refer: https://medium.com/swlh/dockerize-your-python-command-line-program-6a273f5c5544

FROM python:3.9-slim
RUN useradd --create-home --shell /bin/bash app_user
WORKDIR /home/app_user
RUN apt-get update && apt-get install
RUN apt-get install -y r-base
RUN apt-get install -y libcurl4-openssl-dev libssl-dev libfontconfig1-dev libxml2-dev libharfbuzz-dev libfribidi-dev libfreetype6-dev libpng-dev libtiff5-dev libjpeg-dev
RUN Rscript -e 'install.packages("devtools",dependencies=TRUE)'
RUN Rscript -e 'devtools::install_github("jhmadsen/DDoutlier")'
RUN Rscript -e 'install.packages("outlierensembles",dependencies=TRUE)'
RUN mkdir /home/app_user/data
RUN chown -R app_user.app_user /home/app_user/data
USER app_user
COPY . .
RUN python -m venv .venv
RUN .venv/bin/pip install --no-cache-dir -r requirements.txt
CMD ["bash"]
