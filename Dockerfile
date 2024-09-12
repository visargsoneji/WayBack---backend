# Use the Python 3.12.3 base image
FROM python:3.12.3-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements.txt first to leverage Docker cache during builds
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . .

# Expose the port on which the backend will run (change if necessary)
EXPOSE 8000

# Run the application using Uvicorn
CMD ["fastapi", "run", "app/main.py", "--port", "8000"]
  