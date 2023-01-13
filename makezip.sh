#!/bin/bash
PROJECT_DIR=$(PWD)

cd venv/lib/python3.9/site-packages

zip -r deployment.zip . \
    -x ./setuptools\*/\* \
    -x ./pip\*/\* \
    -x \*__pycache__/\* \
    -x ./pkg_resources/\* \
    -x ./_distutils_hack/\* \
    -x .DS_Store

mv deployment.zip $PROJECT_DIR/deployment.zip

cd $PROJECT_DIR

zip -g deployment.zip lambda_function.py
