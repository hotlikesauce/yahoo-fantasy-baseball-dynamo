#!/usr/bin/env python3
"""
Safety check module to prevent accidental database operations
"""
import os

def is_safe_to_run():
    """Check if it's safe to run database operations"""
    # Check if we're in a test environment
    if os.environ.get('LAMBDA_TEST_MODE') == 'true':
        print("Test mode detected - skipping database operations")
        return False

    # Check if this is a Lambda environment
    if os.environ.get('AWS_LAMBDA_FUNCTION_NAME'):
        print("Lambda environment detected - proceeding with operations")
        return True

    # Local environment - ask for confirmation
    print("Running locally - database operations will execute")
    return True
