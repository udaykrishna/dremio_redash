try:
    import pyodbc
    enabled = True
except ImportError:
    enabled = False

import os, json
from redash.query_runner import BaseQueryRunner, register
from redash.query_runner import TYPE_STRING, TYPE_DATE, TYPE_DATETIME, TYPE_INTEGER, TYPE_FLOAT, TYPE_BOOLEAN
from redash.utils import json_dumps, json_loads
import re
import requests
import time

RE_ANNOTATION = re.compile(r"^\/\*(\*(?!\/)|[^*])*\*\/")

TYPES_MAP = {
    0: TYPE_INTEGER,
    1: TYPE_FLOAT,
    2: TYPE_STRING,
    3: TYPE_DATE,
    4: TYPE_DATETIME,
    5: TYPE_STRING,
    6: TYPE_DATETIME,
    13: TYPE_BOOLEAN
}

class DremioConnectionManager:
    endpoint_map = {
            "login":"apiv2/login",
            "new_query": "apiv2/datasets/new_untitled_sql?newVersion=1"
        }
    error_template = "Error {message} at Line {line} and column {col}"
    def __init__(self, host, username, password, odbc_port=31010, api_port=9047, https=False):
        self.host = host
        self.username = username
        self.password = password
        self.api_port = api_port
        self.odbc_port = odbc_port
        self.session = self._create_session()
        self.https=https
        self._login()
    
    @property
    def apiurl(self):
        protocol = 'https' if self.https else 'http'
        return "{protocol}://{host}:{port}".format(protocol=protocol, host=self.host, port=self.api_port)

    def _create_session(self):
        headers = {'content-type':'application/json'}
        session = requests.Session()
        session.headers.update(headers)
        return session
    
    def _login(self):
        endpoint = self.get_url("login")
        resp = self.session.post(endpoint, data=json.dumps({"userName":self.username,"password":self.password}))
        data = json.loads(resp.content.decode())
        self.session.headers.update({"authorization":"_dremio{token}".format(token=data.get("token","invalidtoken"))})
        return self.session
    
    def get_url(self, endpoint_name):
        return "{apiurl}/{endpoint}".format(apiurl=self.apiurl, endpoint=self.endpoint_map[endpoint_name])

    def get_connection_string(self):
        driver = "{" + os.getenv("DREMIO_DRIVER", "Dremio ODBC Driver 64-bit") + "}"
        return "Driver={};ConnectionType=Direct;HOST={};PORT={};AuthenticationType=Plain;UID={};PWD={}".format(
            driver,
            self.host,
            self.odbc_port,
            self.username,
            self.password
        )
    def get_error_message(self, query, _try=0, _max_tries=3):
        url = self.get_url("new_query")
        query = RE_ANNOTATION.sub('', query).strip()
        payload = json.dumps({"sql":query})
        resp = self.session.post(url, data=payload)
        if resp.status_code==400:
            data = json.loads(resp.content.decode())
            code = data.get("code")
            message = data.get("errorMessage")
            code = data.get("code")
            details = []
            for error in data.get("details", {}).get("errors", []):
                e_message = error.get("message",u" \n ").encode("ascii", "ignore")#.split('\n')[0]
                if isinstance(e_message, str):
                    e_message_details = e_message.split('\n')[0]
                    start = e_message.find("org.apache.calcite.sql.parser.SqlParseException")
                    end = e_message[start:].find("com.dremio.exec.planner.sql.parser")
                    if end>-1 and start >-1:
                        e_message = e_message[start+48:start+end]
                    else:
                        e_message = e_message.split('\n')[0]                    
                e_range = error.get("range", {})
                line = e_range.get("startLine")
                column = e_range.get("startColumn")
                details.append(self.error_template.format(message=" ".join([e_message_details, e_message]),
                                                          line=line, col=column))
            details = "\n".join(details)
            base_error = "{code}: {message} \n\nDETAILS\n\n{details}".format(code=code, message=message, details=details)
            return base_error
        elif resp.status_code==200:
            return "Dremio had a slight hiccup, please re-run your query"
        elif resp.status_code==401:
            self._login()
            if _try<_max_tries:
                return self.get_error_message(query, _try=_try+1)
            else:
                return "Unable to Login to dremio"
        else:
            data = resp.content.decode()
            return data



class DremioODBC(BaseQueryRunner):
    noop_query = "SELECT 1"

    def __init__(self, configuration):
        super(DremioODBC, self).__init__(configuration)
                
        self.connection_manager = DremioConnectionManager(self.configuration['host'],
                self.configuration['user'],
                self.configuration['password'],
                self.configuration['port'])

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "driver": {
                    "type": "string",
                    "default": "{Dremio ODBC Driver 64-bit}"
                },
                "host": {
                    "type": "string"
                },
                "port": {
                    "type": "string",
                    "default": "31010"
                },
                "user": {
                    "type": "string"
                },
                "password": {
                    "type": "string"
                },
            },
            "order": ["driver", "host", "port", "user", "password"],
            "required": ["user", "password", "host", "port", "driver"],
            "secret": ["password"]
        }

    @classmethod
    def enabled(cls):
        return enabled

    
    @classmethod
    def type(cls):
        return "dremio_odbc"
    
    @classmethod
    def determine_type(cls, data_type, scale):
        t = TYPES_MAP.get(data_type, None)
        if t == TYPE_INTEGER and scale > 0:
            return TYPE_FLOAT
        return t

    def run_query(self, query, user):
       
        connection = pyodbc.connect(
           self.connection_manager.get_connection_string(),
            autocommit=True
        )

        cursor = connection.cursor()
        start = time.time()
        try:
            cursor.execute(query)

            columns = self.fetch_columns(
                [(i[0], self.determine_type(i[1], i[5])) for i in cursor.description])
            rows = [dict(zip((column['name'] for column in columns), row))
                    for row in cursor]

            data = {'columns': columns, 'rows': rows}
            error = None
            json_data = json_dumps(data)
        except:
            stop = time.time()
            if((stop-start)>1):
                raise ValueError("Dremio query timeout, ensure that your query is optimized and run again")
            else:
                raise ValueError(self.connection_manager.get_error_message(query))
        finally:
            cursor.close()
            connection.close()

        return json_data, error

    def get_schema(self, get_stats=False):
        query = """
        select * from INFORMATION_SCHEMA.COLUMNS
        where not REGEXP_LIKE(TABLE_SCHEMA, '^(sys|__accelerator|INFORMATION_SCHEMA|Samples).*')
        """

        results, error = self.run_query(query, None)

        if error is not None:
            raise Exception("Failed getting schema.")

        schema = {}
        results = json_loads(results)

        for row in results['rows']:
            table_name = '{}.{}'.format(row['TABLE_SCHEMA'], row['TABLE_NAME'])

            if table_name not in schema:
                schema[table_name] = {'name': table_name, 'columns': []}

            schema[table_name]['columns'].append(row['COLUMN_NAME'])

        return list(schema.values())


register(DremioODBC)
