#!/bin/bash
PROJECT_DIR=$(PWD)

cd venv/lib/python3.9/site-packages

# include files in zip other than redis-related files
zip -r deployment.zip . \
    -x \*.csv \
    -x \*.json \
    -x ./setuptools\*/\* \
    -x ./pip\*/\* \
    -x \*__pycache__/\* \
    -x ./pkg_resources/\* \
    -x ./_distutils_hack/\* \
    -x .DS_Store \
    -x ./boto\*/\* \
    -x ./dateutil/\* \
    -x ./jmespath\*/\* \
    -x ./python_dateutil/\* \
    -x ./s3\*/\* \
    -x ./six\*/\* \
    -x ./url\*/\* \
    -x distutils\* \
    -x mccabe\* \
    -x six\*

mv deployment.zip $PROJECT_DIR/deployment.zip

cd $PROJECT_DIR

zip -g deployment.zip lambda_function.py
