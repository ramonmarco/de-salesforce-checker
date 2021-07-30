FROM python:3.6

# Set application name
ARG APP_NAME=de-salesforce-checker

# Install pipenv
RUN pip install pipenv==2018.11.26

# Install dependencies before copy source file to cache dependencies
RUN mkdir -p /root/${APP_NAME}/
WORKDIR /root/${APP_NAME}/
COPY ./Pipfile* ./
ENV PIPENV_MAX_RETRIES=3
ENV PIPENV_TIMEOUT=600
RUN pipenv install --system --deploy

# Copy source files
COPY . .

ENTRYPOINT ["python"]