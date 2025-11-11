# Databricks notebook source
# MAGIC %md Test runner for `pytest`

# COMMAND ----------

!cp ../requirements.txt ~/.
%pip install --quiet -r  ~/requirements.txt
dbutils.library.restartPython() 

# COMMAND ----------



# COMMAND ----------

# Restart python to clear cache
dbutils.library.restartPython()

import pytest
import os
import sys

# Run all tests in the repository root.
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
repo_root = os.path.dirname(os.path.dirname(notebook_path))
os.chdir(f'/Workspace/{repo_root}')
%pwd

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

retcode = pytest.main([".", "-p", "no:cacheprovider"])

# Fail the cell execution if we have any test failures.
assert retcode == 0, 'The pytest invocation failed. See the log above for details.'
