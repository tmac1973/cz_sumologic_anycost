FROM public.ecr.aws/lambda/python:3.13

# Copy application files
COPY sumo_anycost_lambda.py ${LAMBDA_TASK_ROOT}/
COPY requirements.txt ${LAMBDA_TASK_ROOT}/

# Install dependencies
RUN pip install -r requirements.txt

# Set the CMD to your handler
CMD ["sumo_anycost_lambda.lambda_handler"]