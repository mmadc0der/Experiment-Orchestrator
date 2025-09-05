"""
Test runner and utilities for the Experiment Orchestrator test suite.
"""

import pytest
import sys
import os
from pathlib import Path
from typing import List, Optional

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from tests.test_utils import TestWorkspaceManager, TestDataGenerator


class TestRunner:
    """Test runner with utilities for running different test suites."""
    
    def __init__(self):
        self.workspace_manager = TestWorkspaceManager()
    
    def run_unit_tests(self, verbose: bool = True) -> int:
        """Run unit tests only."""
        args = ["-m", "unit"]
        if verbose:
            args.append("-v")
        return pytest.main(args)
    
    def run_integration_tests(self, verbose: bool = True) -> int:
        """Run integration tests only."""
        args = ["-m", "integration"]
        if verbose:
            args.append("-v")
        return pytest.main(args)
    
    def run_all_tests(self, verbose: bool = True) -> int:
        """Run all tests."""
        args = []
        if verbose:
            args.append("-v")
        return pytest.main(args)
    
    def run_specific_test(self, test_path: str, verbose: bool = True) -> int:
        """Run a specific test file or test function."""
        args = [test_path]
        if verbose:
            args.append("-v")
        return pytest.main(args)
    
    def run_tests_with_coverage(self, verbose: bool = True) -> int:
        """Run tests with coverage reporting."""
        args = ["--cov=.", "--cov-report=html", "--cov-report=term"]
        if verbose:
            args.append("-v")
        return pytest.main(args)
    
    def run_fast_tests(self, verbose: bool = True) -> int:
        """Run only fast tests (exclude slow tests)."""
        args = ["-m", "not slow"]
        if verbose:
            args.append("-v")
        return pytest.main(args)
    
    def run_tests_parallel(self, num_workers: int = 4, verbose: bool = True) -> int:
        """Run tests in parallel."""
        try:
            import pytest_xdist
            args = [f"-n{num_workers}"]
            if verbose:
                args.append("-v")
            return pytest.main(args)
        except ImportError:
            print("pytest-xdist not installed. Running tests sequentially.")
            return self.run_all_tests(verbose)
    
    def cleanup(self):
        """Clean up test resources."""
        self.workspace_manager.cleanup_all()


def main():
    """Main test runner entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Experiment Orchestrator Test Runner")
    parser.add_argument("--type", choices=["unit", "integration", "all", "fast"], 
                       default="all", help="Type of tests to run")
    parser.add_argument("--test", help="Specific test file or function to run")
    parser.add_argument("--coverage", action="store_true", help="Run with coverage reporting")
    parser.add_argument("--parallel", type=int, help="Run tests in parallel with N workers")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument("--cleanup", action="store_true", help="Clean up test resources")
    
    args = parser.parse_args()
    
    runner = TestRunner()
    
    try:
        if args.cleanup:
            runner.cleanup()
            print("Test resources cleaned up.")
            return 0
        
        if args.test:
            result = runner.run_specific_test(args.test, args.verbose)
        elif args.coverage:
            result = runner.run_tests_with_coverage(args.verbose)
        elif args.parallel:
            result = runner.run_tests_parallel(args.parallel, args.verbose)
        elif args.type == "unit":
            result = runner.run_unit_tests(args.verbose)
        elif args.type == "integration":
            result = runner.run_integration_tests(args.verbose)
        elif args.type == "fast":
            result = runner.run_fast_tests(args.verbose)
        else:  # all
            result = runner.run_all_tests(args.verbose)
        
        return result
    
    finally:
        runner.cleanup()


if __name__ == "__main__":
    sys.exit(main())


class TestSuite:
    """Test suite configuration and utilities."""
    
    UNIT_TESTS = [
        "tests.test_config_validator",
        "tests.test_manifest_parser",
        "tests.test_models",
        "tests.test_redis_broker",
        "tests.test_orchestrator_core"
    ]
    
    INTEGRATION_TESTS = [
        "tests.test_integration"
    ]
    
    ALL_TESTS = UNIT_TESTS + INTEGRATION_TESTS
    
    @classmethod
    def get_test_modules(cls, test_type: str = "all") -> List[str]:
        """Get list of test modules for specified test type."""
        if test_type == "unit":
            return cls.UNIT_TESTS
        elif test_type == "integration":
            return cls.INTEGRATION_TESTS
        else:
            return cls.ALL_TESTS
    
    @classmethod
    def validate_test_environment(cls) -> bool:
        """Validate that the test environment is properly set up."""
        try:
            # Check if required modules can be imported
            from config_validator import ConfigValidator
            from manifest_processing.manifest_parser import ManifestParser
            from brokers.redis_broker import RedisBroker
            from models.definitions import TaskDefinition
            from models.instances import InstanceStatus
            
            # Check if test utilities work
            generator = TestDataGenerator()
            manifest = generator.create_task_manifest()
            assert "kind: Task" in manifest
            
            return True
        except Exception as e:
            print(f"Test environment validation failed: {e}")
            return False


def run_test_suite(test_type: str = "all", verbose: bool = True) -> int:
    """Run the specified test suite."""
    if not TestSuite.validate_test_environment():
        print("Test environment validation failed. Please check your setup.")
        return 1
    
    runner = TestRunner()
    
    try:
        if test_type == "unit":
            return runner.run_unit_tests(verbose)
        elif test_type == "integration":
            return runner.run_integration_tests(verbose)
        else:
            return runner.run_all_tests(verbose)
    finally:
        runner.cleanup()


# Convenience functions for common test scenarios
def run_quick_tests() -> int:
    """Run quick tests (unit tests only, no slow tests)."""
    return run_test_suite("unit", verbose=True)


def run_full_tests() -> int:
    """Run full test suite."""
    return run_test_suite("all", verbose=True)


def run_integration_only() -> int:
    """Run integration tests only."""
    return run_test_suite("integration", verbose=True)


# Test data generation utilities
def generate_test_manifests(output_dir: Path) -> None:
    """Generate test manifests for manual testing."""
    generator = TestDataGenerator()
    
    manifests = {
        "task.yaml": generator.create_task_manifest(),
        "experiment.yaml": generator.create_experiment_manifest(),
        "environment.yaml": generator.create_environment_manifest(),
        "data.yaml": generator.create_data_manifest(),
        "model.yaml": generator.create_model_manifest(),
        "invalid.yaml": generator.create_invalid_manifest("invalid_kind")
    }
    
    output_dir.mkdir(exist_ok=True)
    
    for filename, content in manifests.items():
        manifest_path = output_dir / filename
        with open(manifest_path, 'w') as f:
            f.write(content)
        print(f"Generated {manifest_path}")


if __name__ == "__main__":
    main()