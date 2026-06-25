# histdatacom Samples

These examples are the user-facing API samples referenced by `README.md`.
Pytest executes them in hermetic mode so the documented script and notebook paths
stay valid without contacting HistData.com or starting a Temporal runtime.

- `api_quickstart.py` shows script/application options and dataframe-return API
  options.
- `notebooks/api_quickstart.ipynb` shows the same dataframe-return path in a
  Jupyter notebook.

Running the samples normally uses the real `histdatacom(options)` API and may
start the local orchestration runtime and request data from HistData.com.

