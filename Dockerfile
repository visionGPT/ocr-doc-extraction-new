FROM python:3.11.4-slim

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libpoppler-cpp-dev \
    poppler-utils \
    wkhtmltopdf \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create necessary directories
RUN mkdir -p static converted_files models templates temp_downloads default_files document_templates\samples processing_results

# Download YOLO model (if needed)
RUN python -c "from ultralytics import YOLO; YOLO('models/yolov10x_best.pt')"

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PORT=8000


# Expose port
EXPOSE 8000

# Command to run the application
CMD ["uvicorn", "Opticintellect_updated:app", "--host", "0.0.0.0", "--port", "8000"]
