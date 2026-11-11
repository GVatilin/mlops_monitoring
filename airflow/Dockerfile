FROM apache/airflow:2.10.5-python3.11

USER airflow
RUN pip install --no-cache-dir \
    "numpy==1.26.4" \
    "pandas==2.1.4" \
    "scipy==1.14.1" \
    "joblib==1.4.2" \
    "scikit-learn==1.5.2" \
    "catboost==1.2.8" \
    "cloudpickle==2.2.1" \
    && python -c "import catboost, sklearn, pandas, pyarrow, numpy, cloudpickle; print(catboost.__version__, numpy.__version__, pandas.__version__, cloudpickle.__version__)"

USER root
COPY entrypoint.sh /custom-entrypoint.sh
RUN chmod +x /custom-entrypoint.sh

ENTRYPOINT ["/custom-entrypoint.sh"]
