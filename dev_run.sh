#!/bin/bash
source venv/bin/activate
export IS_DEV=true
python3 lambda_function.py
