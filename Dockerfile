FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm AS development
#tag for dev
# Devcontainers dont offer slim versions due to philosophical reasons (assume dev container enviroment needs lots of extra tools like gcc, git, etc). Otherwise we could have used the same base image

# Switch to the pre-existing vscode user
USER vscode

# Set working directory
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# run process indefinitely to keep the container alive for development
CMD ["tail", "-f", "/dev/null"]

FROM python:3.12-slim-bookworm AS production
#tag for prod 

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

#Here we copy the code into the image, wheres in the development stage we just mount it
COPY . .

CMD ["python", "etl_pipeline.py"]
