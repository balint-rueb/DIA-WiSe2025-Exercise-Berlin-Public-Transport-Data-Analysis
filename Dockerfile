FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm
# refrences the current OS version of Debian (called bookworm). Devcontainers dont offer slim versions due to philosophical reasons (assume dev container enviroment needs lots of extra tools like gcc, git, etc).
# The python version here should be identical to the back-end python version

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Ensure vscode user exists (common in devcontainer base images)
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=$USER_UID

# Create or update the vscode user (if not already done)
# Safely create group and user if they don't already exist
RUN groupadd --gid 1000 vscode 2>/dev/null || true && \
    id -u vscode &>/dev/null || useradd --uid 1000 --gid 1000 -m vscode && \
    apt-get update && \
    apt-get install -y sudo && \
    echo vscode ALL=\(root\) NOPASSWD:ALL > /etc/sudoers.d/vscode && \
    chmod 0440 /etc/sudoers.d/vscode


# Switch to vscode user
USER $USERNAME

# Set working directory
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# Run the app
CMD ["tail", "-f", "/dev/null"]
#CMD ["sh", "-c", "ollama serve & sleep 10 && ollama run llama3.2 && streamlit run app.py --server.port 8080"]
