#!/usr/bin/env python3
"""
Simple test runner script for the Experiment Orchestrator.
"""

import sys
import subprocess
from pathlib import Path

def run_command(cmd, description):
    """Run a command and return success status."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {' '.join(cmd)}")
    print('='*60)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print("✅ SUCCESS")
        if result.stdout:
            print("STDOUT:")
            print(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        print("❌ FAILED")
        print(f"Return code: {e.returncode}")
        if e.stdout:
            print("STDOUT:")
            print(e.stdout)
        if e.stderr:
            print("STDERR:")
            print(e.stderr)
        return False
    except FileNotFoundError:
        print("❌ FAILED - Command not found")
        return False

def main():
    """Main test runner."""
    print("🧪 Experiment Orchestrator Test Runner")
    print("=" * 60)
    
    # Check if we're in the right directory
    if not Path("orchestrator_core.py").exists():
        print("❌ Error: Please run this script from the project root directory")
        sys.exit(1)
    
    # Check if pytest is available
    try:
        import pytest
        print(f"✅ pytest version: {pytest.__version__}")
    except ImportError:
        print("❌ Error: pytest not installed. Please install test dependencies:")
        print("   pip install -r requirements-test.txt")
        sys.exit(1)
    
    # Test commands to run
    tests = [
        {
            "cmd": ["python", "-m", "pytest", "tests/test_config_validator.py", "-v"],
            "description": "Configuration Validator Tests"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/test_manifest_parser.py", "-v"],
            "description": "Manifest Parser Tests"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/test_models.py", "-v"],
            "description": "Data Models Tests"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/test_redis_broker.py", "-v"],
            "description": "Redis Broker Tests"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/test_orchestrator_core.py", "-v"],
            "description": "Orchestrator Core Tests"
        },
        {
            "cmd": ["python", "-m", "pytest", "tests/test_integration.py", "-v", "-m", "integration"],
            "description": "Integration Tests"
        }
    ]
    
    # Run tests
    success_count = 0
    total_tests = len(tests)
    
    for test in tests:
        if run_command(test["cmd"], test["description"]):
            success_count += 1
    
    # Summary
    print(f"\n{'='*60}")
    print(f"TEST SUMMARY")
    print(f"{'='*60}")
    print(f"Tests passed: {success_count}/{total_tests}")
    
    if success_count == total_tests:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed!")
        return 1

if __name__ == "__main__":
    sys.exit(main())