from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
from typing import List
import os
from dotenv import load_dotenv


load_dotenv()


DB_NAME = os.environ['DB_NAME']
USER = os.environ['USER']
PASSWORD = os.environ['PASSWORD']
HOST = os.environ['HOST']
PORT = os.environ['PORT']


app = FastAPI()

#database connection parameters
DB_PARAMS = {
    'dbname': DB_NAME,
    'user': USER,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT
}
print (DB_PARAMS)
#Establish connection to the PostgresSQL DB

def connect():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# create employees table if it does not exist

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
        raise HTTPException(status_code=500, detail = str(e))
    

# Middleware to ensure the table is created
@app.middleware("http")
async def db_middleware(request: Request, call_next):
    create_employees_table()
    response = await call_next(request)
    return response


class Employee(BaseModel):
    employee_id:int 
    name: str
    age: int
    department: str 

class EmployeeUpdate(BaseModel):
    name: str = None
    age: int = None
    department: str = None

@app.post("/employee/")
def create_employee(employee: Employee):
    try:
        conn = connect()
        cur = conn.cursor()
        query = """
                INSERT INTO employees (employee_id, name, age, department)
                VALUES (%s, %s, %s, %s)
                RETURNING employee_id
            """
        cur.execute(query, (employee.employee_id, employee.name, employee.age, employee.department))
        employee_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        conn.close()
        return {**employee.model_dump(), "employee_id":employee_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@app.get("/employees/", response_model=List[Employee])
def read_employees():
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