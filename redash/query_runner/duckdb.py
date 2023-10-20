# %%
import logging
import duckdb
import os
import re
from kwpipeline.transform import Input, Environment

from redash.query_runner import (
    BaseSQLQueryRunner,
    JobTimeoutException,
    register,
)
from redash.utils import json_dumps, json_loads

logger = logging.getLogger(__name__)

# %%

r = re.compile('kw://([\w/]*)')

def prepare_query(a):

    def make_inputs(match):
        dataset = match[1]
        return f"s3://knotwerk-data/{dataset}/*.parquet"

    result_sql =  r.sub(make_inputs, a)    
    return result_sql

# %%
class DuckDB(BaseSQLQueryRunner):
    noop_query = "pragma quick_check"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {"branch": {"type": "string", "title": "Branch", "default": "knotwerk-data"}}
        }

    @classmethod
    def type(cls):
        return "duckdb"

    def __init__(self, configuration):
        super(DuckDB, self).__init__(configuration)

        access_key = os.environ.get("AWS_ACCESS_KEY_ID")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        endpoint = os.environ.get("AWS_ENDPOINT_URL")

        script = f"""
        install 'httpfs';
        load 'httpfs';

        SET s3_endpoint='{endpoint}';
        SET s3_region='eu-central-1';
        set s3_access_key_id='{access_key}';
        set s3_secret_access_key='{secret_key}';
        SET s3_url_style='path';
        """

        duckdb.sql(script)
        self.branch = configuration.get("branch", "knotwerk-data")
       

    def _get_tables(self, schema):
        """
        query_table = "select tbl_name from sqlite_master where type='table'"
        query_columns = 'PRAGMA table_info("%s")'

        results, error = self.run_query(query_table, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        results = json_loads(results)

        for row in results["rows"]:
            table_name = row["tbl_name"]
            schema[table_name] = {"name": table_name, "columns": []}
            results_table, error = self.run_query(query_columns % (table_name,), None)
            if error is not None:
                self._handle_run_query_error(error)

            results_table = json_loads(results_table)
            for row_column in results_table["rows"]:
                schema[table_name]["columns"].append(row_column["name"])

        return list(schema.values())
        """
        return []

    def run_query(self, query, user):

        try:
            result = duckdb.sql(prepare_query(query))

            typeConv = {"VARCHAR": "string", "DOUBLE": "float", "BIGINT": "int"}
            columns = []
            
            for (col, dtype) in zip(result.columns, result.dtypes):
                columns.append({"name": col, "type": typeConv[str(dtype)]})

            rows = result.to_df().to_dict('records')

            if len(rows) > 0:
                data = {"columns": columns, "rows": rows}
                error = None
                json_data = json_dumps(data)
            else:
                error = "Query completed but it returned no data."
                json_data = None
        except Exception as e:
            error = f"Query completed but error: {e}"
            json_data = None
        return json_data, error

register(DuckDB)
