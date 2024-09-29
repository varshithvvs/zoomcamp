FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim

# Install Poetry
RUN pip install -U pip && pip install poetry

# Copy the Poetry files
COPY ["pyproject.toml", "poetry.lock", "./"]

# Install dependencies
RUN poetry install --no-root

# Copy the batch script
COPY ["module_4/homework.py", "homework.py"]

# Set the entrypoint to use Poetry and pass arguments
ENTRYPOINT ["poetry", "run", "python", "homework.py"]

# Default arguments (can be overridden)
CMD ["2023", "5"]