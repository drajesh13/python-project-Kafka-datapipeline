import psycopg2

# Configuring database connection
DB_CONFIG = {
    "dbname": "Inventory_management",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",  # Use 'localhost' if running PostgreSQL locally
    "port": "5432"        # Default PostgreSQL port
}

def fetch_data(query):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            print(row)

        cursor.close()
        conn.close()
    except Exception as e:
        print("Error:", e)

def move_data():
    """Fetches data from Employee_table and Department_table and inserts into Emp_department."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Fetch employee and department data using JOIN
        fetch_query = """
            SELECT e.id, e.first_name, e.last_name, e.age, e.department, e.salary, e.hire_date, 
                   d.department_id AS dept_id, d.manager_name, d.location, d.budget
            FROM Employee_table e
            JOIN Department_table d ON e.department = d.department;
        """
        
        cursor.execute(fetch_query)
        records = cursor.fetchall()

        # Insert data into Emp_department table
        insert_query = """
            INSERT INTO Emp_department (id, first_name, last_name, age, department, salary, hire_date, department_id, manager_name, location, budget)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        for row in records:
            cursor.execute(insert_query, row)
        
        conn.commit()
        print(f"Successfully moved {cursor.rowcount} records to Emp_department.")

        cursor.close()
        conn.close()

    except Exception as e:
        print("Error:", e)

# Execute fetch functions
print("=== Employee Table Data ===")
fetch_data("SELECT * FROM Employee_table;")

print("\n=== Department Table Data ===")
fetch_data("SELECT * FROM Department_table;")

# Move Data from Employee_table and Department_table to Emp_department
print("\n=== Moving Data to Emp_department ===")
move_data()

# Fetch and display the moved data from Emp_department
print("\n=== Emp_department Table Data ===")
fetch_data("SELECT * FROM Emp_department;")