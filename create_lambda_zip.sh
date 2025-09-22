#!/bin/bash

# create_lambda_zip.sh
# Script to create a Lambda deployment zip file with all dependencies

set -e  # Exit on any error

# Configuration
LAMBDA_ZIP_NAME="sumo-anycost-lambda.zip"
PACKAGE_DIR="lambda_package"
PYTHON_VERSION="python3.13"

echo "=== Creating Lambda deployment zip ==="
echo "Python version: $PYTHON_VERSION"
echo "Output zip: $LAMBDA_ZIP_NAME"

# Clean up any existing package directory
if [ -d "$PACKAGE_DIR" ]; then
    echo "Removing existing package directory..."
    rm -rf "$PACKAGE_DIR"
fi

# Create package directory
echo "Creating package directory..."
mkdir -p "$PACKAGE_DIR"

# Use UV to install dependencies with Linux platform targeting
echo "Installing dependencies for Linux Lambda runtime using UV..."

# Use UV's pip to install lightweight dependencies only (no pandas/numpy)
echo "Installing lightweight Linux-compatible packages..."
uv pip install \
    certifi==2025.7.9 \
    charset-normalizer==3.4.2 \
    idna==3.10 \
    requests==2.32.4 \
    urllib3==2.5.0 \
    --target "$PACKAGE_DIR" \
    --python-platform x86_64-unknown-linux-gnu \
    --only-binary=:all: || \
uv pip install \
    certifi charset-normalizer idna requests urllib3 \
    --target "$PACKAGE_DIR" \
    --only-binary=:all:

echo "Successfully installed lightweight dependencies (no pandas/numpy for smaller package)"

# Copy the main Lambda function and rename it to lambda_function.py
echo "Copying main Lambda function as lambda_function.py..."
cp sumo_anycost_lambda.py "$PACKAGE_DIR/lambda_function.py"

# Remove unnecessary files to reduce package size
echo "Cleaning up unnecessary files..."
find "$PACKAGE_DIR" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find "$PACKAGE_DIR" -type f -name "*.pyc" -delete 2>/dev/null || true
find "$PACKAGE_DIR" -type f -name "*.pyo" -delete 2>/dev/null || true
find "$PACKAGE_DIR" -type d -name "*.dist-info" -exec rm -rf {} + 2>/dev/null || true
find "$PACKAGE_DIR" -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true

# Remove test directories
find "$PACKAGE_DIR" -type d -name "tests" -exec rm -rf {} + 2>/dev/null || true
find "$PACKAGE_DIR" -type d -name "test" -exec rm -rf {} + 2>/dev/null || true

# Fix numpy import issues by removing problematic files/directories
echo "Fixing numpy import issues..."
# Remove any numpy source directories that might conflict
find "$PACKAGE_DIR" -name "numpy" -type d -path "*/site-packages/*" -exec rm -rf {} + 2>/dev/null || true
# Remove setup.py files that might cause import confusion
find "$PACKAGE_DIR" -name "setup.py" -delete 2>/dev/null || true
# Remove any .pth files that might redirect imports
find "$PACKAGE_DIR" -name "*.pth" -delete 2>/dev/null || true

# Create zip file
echo "Creating zip file..."
cd "$PACKAGE_DIR"
zip -r "../$LAMBDA_ZIP_NAME" . -q
cd ..

# Get zip file size
ZIP_SIZE=$(du -h "$LAMBDA_ZIP_NAME" | cut -f1)

echo ""
echo "=== Lambda zip creation completed ==="
echo "Zip file: $LAMBDA_ZIP_NAME"
echo "Size: $ZIP_SIZE"
echo ""

# Check if zip is within Lambda limits
ZIP_SIZE_BYTES=$(stat -f%z "$LAMBDA_ZIP_NAME" 2>/dev/null || stat -c%s "$LAMBDA_ZIP_NAME" 2>/dev/null)
if [ "$ZIP_SIZE_BYTES" -gt 52428800 ]; then  # 50MB limit for direct upload
    echo "⚠️  WARNING: Zip file ($ZIP_SIZE) exceeds 50MB limit for direct console upload."
    echo "   You'll need to upload via S3 or use the container image approach."
else
    echo "✅ Zip file size is within Lambda limits for direct upload."
fi

echo ""
echo "Contents of the zip file:"
unzip -l "$LAMBDA_ZIP_NAME" | head -20
echo "..."
echo "(showing first 20 entries)"

# Clean up package directory
echo ""
echo "Cleaning up temporary directory..."
rm -rf "$PACKAGE_DIR"

echo ""
echo "🎉 Lambda deployment zip created successfully!"
echo "Upload $LAMBDA_ZIP_NAME to AWS Lambda."