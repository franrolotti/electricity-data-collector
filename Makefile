install:
\tpip install -e . && pip install -r airflow/requirements.txt

up:
\tdocker compose up -d

down:
\tdocker compose down

logs:
\tdocker compose logs -f airflow

test:
\tpytest -q
