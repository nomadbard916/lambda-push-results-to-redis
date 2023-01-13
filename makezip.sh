#!/bin/bash
zip -r deployment.zip venv/lib/python3.9/site-packages;
zip -g deployment.zip lambda_function.py
