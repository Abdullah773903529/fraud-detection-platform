# FROM quay.io/astronomer/astro-runtime:11.0.0

# USER root

# # ============================================
# # تثبيت الحزم مباشرة (بدون install-system-packages)
# # ============================================
# RUN apt-get update && \
#     apt-get install -y wget curl procps && \
#     rm -rf /var/lib/apt/lists/*

# # ============================================
# # تثبيت حزم بايثون
# # ============================================
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # ============================================
# # إعدادات إضافية
# # ============================================
# RUN mkdir -p /tmp/checkpoints /opt/scripts
# ENV PYTHONPATH="${PYTHONPATH}:/usr/local/airflow/plugins"

# USER astro



FROM quay.io/astronomer/astro-runtime:11.0.0

USER root

# ============================================
# System packages
# ============================================
RUN apt-get update && \
    apt-get install -y wget curl procps && \
    rm -rf /var/lib/apt/lists/*

# ============================================
# Python dependencies
# ============================================
COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /requirements.txt

# ============================================
# Remove broken Astronomer UI plugin (IMPORTANT)
# ============================================
RUN pip uninstall -y astronomer-airflow-version-check || true

# ============================================
# Directories & env
# ============================================
RUN mkdir -p /tmp/checkpoints /opt/scripts

ENV PYTHONPATH="/usr/local/airflow/plugins:${PYTHONPATH}"

# ============================================
# back to non-root
# ============================================
USER astro