from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
from typing import List
import os
from dotenv import load_dotenv
import logging


##This is running

load_dotenv()

DB_NAME = os.environ['DB_NAME']
USER = os.environ['USER']
PASSWORD = os.environ['PASSWORD']
HOST = os.environ['HOST']
PORT = os.environ['PORT']

app = FastAPI()

logging.basicConfig(level=logging.DEBUG)

# Database connection parameters
DB_PARAMS = {
    'dbname': DB_NAME,
    'user': USER,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT
}

# Establish connection to the PostgreSQL DB
def connect():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        logging.error(f"Error connecting to the DB: {e}")
        raise Exception("Database connection error")

# Middleware to ensure the table is created
@app.middleware("http")
async def db_middleware(request: Request, call_next):
    try:
        create_employees_table()
    except Exception as e:
        logging.error(f"Error in middleware during table creation: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    response = await call_next(request)
    return response

class Employee(BaseModel):
    name: str
    age: int
    department: str

class EmployeeUpdate(BaseModel):
    name: str = None
    age: int = None
    department: str = None

def create_employees_table():
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("""
                CREATE TABLE IF NOT EXISTS employees (
                employee_id SERIAL PRIMARY KEY, 
                name VARCHAR(50),
                age INT,
                department VARCHAR(50)
                )
                """)
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise Exception("Table creation error")

@app.post("/employee/")
async def create_employee(employee: Employee):
    try:
        conn = connect()
        cur = conn.cursor()
        query = """
                INSERT INTO employees (name, age, department)
                VALUES (%s, %s, %s)
                RETURNING employee_id
            """
        cur.execute(query, (employee.name, employee.age, employee.department))
        employee_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {**employee.model_dump(), "employee_id": employee_id}
    except Exception as e:
        logging.error(f"Error while inserting data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/employees/", response_model=List[Employee])
async def read_employees():
    try:
        conn = connect()
        cur = conn.cursor()
        query = """SELECT * FROM employees"""
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [Employee(employee_id=row[0], name=row[1], age=row[2], department=row[3]) for row in rows]
    except Exception as e:
        logging.error(f"Error while reading data: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Optional: Endpoint to explicitly create the table
@app.post("/create_table/")
async def create_table():
    try:
        create_employees_table()
        return {"message": "Table created successfully"}
    except Exception as e:
        logging.error(f"Error creating table: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Optional: Endpoint to add employees separately
@app.post("/add_employee/")
async def add_employee(employee: Employee):
    return await create_employee(employee)

# Run the code
if __name__ == "__main__":
    import uvicornfrom fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
from typing import List
import os
from dotenv import load_dotenv
import logging
from cfenv import AppEnv
import json
from os.path import join, dirname

load_dotenv()

"""
DB_NAME = os.environ['DB_NAME']
USERNAME = os.environ['USERNAME']
PASSWORD = os.environ['PASSWORD']
HOST = os.environ['HOST']
PORT = os.environ['PORT']
"""

app = FastAPI()
class AppConfig:
    def __init__(self):
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)
        self.print_env()

        self.LOCAL_ENV = os.getenv("ENV", "PROD").upper() == "LOCAL"

        if self.LOCAL_ENV:
            self.load_local_env()
        else:
            self.load_production_env()


    def load_production_env(self):
        env = AppEnv()

        postgresql = env.get_service(name='postgresql')
        self.DB_USER = postgresql.credentials["username"]
        self.DB_PWD = postgresql.credentials["password"]
        self.DB_URL = postgresql.credentials["hostname"]
        self.DB_PORT = postgresql.credentials["port"]
        self.DB_NAME = postgresql.credentials["dbname"]
        self.DB_CONN_URL = f"postgresql+asyncpg://{self.DB_USER}:{self.DB_PWD}@{self.DB_URL}:{self.DB_PORT}/{self.DB_NAME}"

    def to_json(self):
        data = self.__dict__.copy()
        return json.dumps(data, indent=4)
    
    def print_env(self):
        for key, value in os.environ.items():
            print(f"{key}={value}")


db_config = AppConfig()

#database connection parameters
DB_PARAMS = {
    'dbname': db_config.DB_NAME,
    'user': db_config.DB_USER,
    'password': db_config.DB_PWD,
    'host': db_config.DB_URL,
    'port': db_config.DB_PORT
}

print(DB_PARAMS)

class Employee(BaseModel):
    employee_id:int 
    name: str
    age: int
    department: str 

class EmployeeUpdate(BaseModel):
    name: str = None
    age: int = None
    department: str = None

#Establish connection to the PostgresSQL DB

def connect():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def execute_create_queries(connect, create_queries):
    try:
        # Create a cursor object
        cursor = connect.cursor()
 
        # Execute each create query
        for query in create_queries:
            cursor.execute(query)
            print(f"Query executed: {query}")
 
        # Commit the transaction
        connect.commit()
        print("All create queries executed successfully")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")
    finally:
        # Closing cursor
        if cursor:
            cursor.close()

 
def execute_query(connect, query):
    cursor = connect.cursor()
    try:
        cursor.execute(query)
        if cursor.description:
            # Fetch all rows if the query returns any result
            result = cursor.fetchall()
            # Convert result to JSON format
            result_json = json.dumps(result)
            return result_json
        else:
            return "Query executed successfully"
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")
    finally:
        cursor.close()


@app.post('/creat_table/')
def create_employees_table(request: Request):
    print("Creating Table")
    create_queries = [
        """
        CREATE TABLE IF NOT EXISTS employees (
                employee_id SERIAL PRIMARY KEY, 
                name VARCHAR(50),
                age INT,
                department VARCHAR(50)
        );"""
    ]

    # Connect to the database
    connection = connect()
 
    if connection:
        # Execute create table queries
        execute_create_queries(connection, create_queries)
 
        # Close the database connection
        connection.close()
        print("PostgreSQL connection is closed")
        return HTMLResponse("PostgreSQL connection is closed")
    else:
        return HTMLResponse("Error creating Db connection")

@app.post("/employee/")
async def insert_data(employee: Employee):
    print("Inserting data")
    try:
        query = """
            INSERT INTO employees (employee_id, name, age, department)
            VALUES (%s, %s, %s, %s)
            RETURNING employee_id
            """
        # Connect to the database
        connection = connect()
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(query, (employee.employee_id, employee.name, employee.age, employee.department))
                employee_id = cursor.fetchone()[0]
                connection.commit()
                return JSONResponse(content={"employee_id": employee_id})
        else:
            raise HTTPException(status_code=500, detail="Error creating DB connection")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection is closed")


@app.get("/employees/", response_model=List[Employee])
async def read_employees():
    print("Reading the table")
    try:
        conn = connect()
        cur = conn.cursor()
        query = """SELECT * FROM employees"""
        cur.execute(query)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return [Employee(employee_id=row[0], name=row[1], age=row[2], department = row[3]) for row in rows]
    except Exception as e:
        
        raise HTTPException(status_code=500, detail=str(e))


#Run the code
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
    create_employees_table()  # Ensure table creation at startup
    uvicorn.run(app, host="0.0.0.0", port=8080)
