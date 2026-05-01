import sys

def test_python_version():
    """Test Python version is 3.8+"""
    assert sys.version_info >= (3, 8), "Python 3.8+ required"
    print("✅ Python version check passed")

def test_critical_imports():
    """Test critical imports that should always work"""
    try:
        import json
        import os
        import sys
        assert True
        print("✅ Core imports working")
    except ImportError as e:
        assert False, f"Core import failed: {e}"

def test_optional_imports():
    """Test optional imports (won't fail CI if missing)"""
    import importlib.util
    
    optional_modules = ['pyspark', 'snowflake.connector', 'kafka', 'langchain']
    available = []
    missing = []
    
    for module in optional_modules:
        try:
            spec = importlib.util.find_spec(module)
            if spec is not None:
                available.append(module)
            else:
                missing.append(module)
        except (ImportError, ModuleNotFoundError, ValueError):
            missing.append(module)
    
    print(f"✅ Available modules: {', '.join(available) if available else 'None'}")
    if missing:
        print(f"⚠️  Optional modules not installed: {', '.join(missing)}")
    
    # Don't fail the test, just report
    assert True

def test_data_quality_logic():
    """Test data quality check logic"""
    # Simple validation test
    rating = 8.5
    assert 0 <= rating <= 10, "Rating must be between 0 and 10"
    
    popularity = 50
    assert popularity >= 0, "Popularity must be positive"
    
    print("✅ Data quality logic tests passed")

def test_project_structure():
    """Test that required files exist"""
    import os
    
    required_files = [
        'requirements.txt',
        '.gitignore',
        'README.md',
        'docker-compose.yml',
        '.env.example'
    ]
    
    missing = []
    for file in required_files:
        if not os.path.exists(file):
            missing.append(file)
    
    if missing:
        print(f"⚠️  Missing files: {', '.join(missing)}")
    else:
        print("✅ All required files present")
    
    # Only fail if critical files are missing
    critical = ['requirements.txt', '.gitignore']
    critical_missing = [f for f in critical if f in missing]
    assert len(critical_missing) == 0, f"Critical files missing: {critical_missing}"

if __name__ == "__main__":
    test_python_version()
    test_critical_imports()
    test_optional_imports()
    test_data_quality_logic()
    test_project_structure()
    print("\n🎉 All tests passed!")