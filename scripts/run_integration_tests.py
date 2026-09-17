"""Run integration tests using pytest."""
import shutil
import sys
import tempfile
import pytest

def main():
    """Execute pytest with integration marker."""
    workspace_test_path = "/Workspace/Users/yannickkh@outlook.com/fraud-scoring-engine/tests"
    workspace_src_path = "/Workspace/Users/yannickkh@outlook.com/fraud-scoring-engine/src"
    
    # Copy tests and source to /tmp where __pycache__ is supported
    with tempfile.TemporaryDirectory() as tmpdir:
        # Copy tests
        tmp_test_path = f"{tmpdir}/tests"
        shutil.copytree(workspace_test_path, tmp_test_path)
        
        # Copy src so imports work
        tmp_src_path = f"{tmpdir}/src"
        shutil.copytree(workspace_src_path, tmp_src_path)
        
        # Add both src and tests to path so all imports work
        sys.path.insert(0, tmp_src_path)
        sys.path.insert(0, tmp_test_path)  # For spark_helpers.py
        
        # Run pytest in the temp location - pytest can create __pycache__ here
        exit_code = pytest.main(["-m", "integration", tmp_test_path, "-v", "-rs"])
        
        # Raise an exception only if pytest failed
        if exit_code != 0:
            raise RuntimeError(f"Integration tests failed with exit code {exit_code}")

if __name__ == "__main__":
    main()