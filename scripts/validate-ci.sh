#!/bin/bash

# CI/CD Validation Script
# This script validates that the CI/CD pipeline configuration is correct

set -e

echo "🔍 Validating CI/CD Pipeline Configuration..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    if [ $1 -eq 0 ]; then
        echo -e "${GREEN}✅ $2${NC}"
    else
        echo -e "${RED}❌ $2${NC}"
        exit 1
    fi
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

# Check if we're in the right directory
if [ ! -f "requirements.txt" ]; then
    echo -e "${RED}❌ requirements.txt not found. Please run this script from the project root.${NC}"
    exit 1
fi

# Check GitHub Actions workflow
echo "Checking GitHub Actions workflow..."
if [ -f ".github/workflows/ci.yml" ]; then
    print_status 0 "GitHub Actions workflow found"
    
    # Validate YAML syntax
    if command -v yamllint &> /dev/null; then
        yamllint .github/workflows/ci.yml
        print_status 0 "GitHub Actions workflow YAML is valid"
    else
        print_warning "yamllint not found, skipping YAML validation"
    fi
else
    print_status 1 "GitHub Actions workflow not found"
fi

# Check Dockerfile
echo "Checking Dockerfile..."
if [ -f "Dockerfile" ]; then
    print_status 0 "Dockerfile found"
    
    # Basic Dockerfile validation
    if grep -q "FROM python:" Dockerfile; then
        print_status 0 "Dockerfile has Python base image"
    else
        print_status 1 "Dockerfile missing Python base image"
    fi
else
    print_status 1 "Dockerfile not found"
fi

# Check docker-compose.yml
echo "Checking docker-compose.yml..."
if [ -f "docker-compose.yml" ]; then
    print_status 0 "docker-compose.yml found"
    
    # Validate docker-compose syntax
    if command -v docker-compose &> /dev/null; then
        docker-compose config &> /dev/null
        print_status 0 "docker-compose.yml is valid"
    else
        print_warning "docker-compose not found, skipping validation"
    fi
else
    print_status 1 "docker-compose.yml not found"
fi

# Check Makefile
echo "Checking Makefile..."
if [ -f "Makefile" ]; then
    print_status 0 "Makefile found"
    
    # Check for essential targets
    if grep -q "test:" Makefile; then
        print_status 0 "Makefile has test target"
    else
        print_status 1 "Makefile missing test target"
    fi
else
    print_status 1 "Makefile not found"
fi

# Check pre-commit configuration
echo "Checking pre-commit configuration..."
if [ -f ".pre-commit-config.yaml" ]; then
    print_status 0 "pre-commit configuration found"
    
    # Validate YAML syntax
    if command -v yamllint &> /dev/null; then
        yamllint .pre-commit-config.yaml
        print_status 0 "pre-commit configuration YAML is valid"
    else
        print_warning "yamllint not found, skipping YAML validation"
    fi
else
    print_status 1 "pre-commit configuration not found"
fi

# Check pytest configuration
echo "Checking pytest configuration..."
if [ -f "pytest.ini" ]; then
    print_status 0 "pytest.ini found"
else
    print_status 1 "pytest.ini not found"
fi

# Check test files
echo "Checking test files..."
if [ -d "tests" ] && [ "$(find tests -name "test_*.py" | wc -l)" -gt 0 ]; then
    print_status 0 "Test files found"
    
    # Count test files
    test_count=$(find tests -name "test_*.py" | wc -l)
    echo "  Found $test_count test files"
else
    print_status 1 "No test files found in tests/ directory"
fi

# Check requirements files
echo "Checking requirements files..."
if [ -f "requirements.txt" ]; then
    print_status 0 "requirements.txt found"
else
    print_status 1 "requirements.txt not found"
fi

if [ -f "requirements-test.txt" ]; then
    print_status 0 "requirements-test.txt found"
else
    print_status 1 "requirements-test.txt not found"
fi

# Check Python syntax
echo "Checking Python syntax..."
if command -v python3 &> /dev/null; then
    # Find all Python files and check syntax
    python_files=$(find . -name "*.py" -not -path "./venv/*" -not -path "./.venv/*" -not -path "./tests/__pycache__/*")
    syntax_errors=0
    
    for file in $python_files; do
        if ! python3 -m py_compile "$file" 2>/dev/null; then
            echo -e "${RED}❌ Syntax error in $file${NC}"
            syntax_errors=$((syntax_errors + 1))
        fi
    done
    
    if [ $syntax_errors -eq 0 ]; then
        print_status 0 "All Python files have valid syntax"
    else
        print_status 1 "$syntax_errors Python files have syntax errors"
    fi
else
    print_warning "Python3 not found, skipping syntax check"
fi

# Check if virtual environment exists
echo "Checking virtual environment..."
if [ -d "venv" ] || [ -d ".venv" ]; then
    print_status 0 "Virtual environment found"
else
    print_warning "Virtual environment not found. Run 'make install' to create one."
fi

# Summary
echo ""
echo "🎉 CI/CD Pipeline Validation Complete!"
echo ""
echo "Next steps:"
echo "1. Run 'make install' to set up the development environment"
echo "2. Run 'make test' to verify all tests pass"
echo "3. Run 'make check' to run all quality checks"
echo "4. Set up pre-commit hooks with 'pre-commit install'"
echo ""
echo "For more information, see CI_CD_README.md"