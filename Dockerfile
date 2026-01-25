# for devcontainer
FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm AS development

# Switch to root to install system-level packages
USER root

# Fix for expired Yarn GPG key: Remove the yarn repo entirely
RUN rm -f /etc/apt/sources.list.d/yarn.list

# install Java, needed for task 3
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# set java env variables so spark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Set working directory and switch back to vscode user for safety
WORKDIR /app
USER vscode

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["tail", "-f", "/dev/null"]


# for eval
FROM python:3.12-slim-bookworm AS production

# java needed for task 3
RUN apt-get update && apt-get install -y \
    openjdk-17-jre-headless \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set Java Environment Variables for Production
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

# Install Python requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the code
COPY . .

# Run migrations and start shell
CMD ["/bin/sh", "-c", "alembic upgrade head && /bin/bash"]