import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)
    return conn

def execute_sql(conn, sql):
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(f"Failed to execute sql: {sql}")
        print(e)
        raise e

create_person_sql = """
    CREATE TABLE IF NOT EXISTS person (
        id integer PRIMARY KEY,
        imie text NOT NULL,
        nazwisko text,
        email text
    );
    """

create_tasks_sql = """
    CREATE TABLE IF NOT EXISTS tasks (
        id integer PRIMARY KEY,
        person_id integer NOT NULL,
        nazwa VARCHAR(250) NOT NULL,
        opis TEXT,
        status VARCHAR(15) NOT NULL,
        start_date text NOT NULL,
        end_date text NOT NULL,
        FOREIGN KEY (person_id) REFERENCES person (id)
    );
    """

db_file = "my_database.db"

conn = create_connection(db_file)
if conn is not None:
    execute_sql(conn, create_person_sql)
    execute_sql(conn, create_tasks_sql)
    conn.close()


def add_person(conn, person):
   """
   Create a new persona into the person table
   :param conn:
   :param person:
   :return: person id
   """
   sql = '''INSERT INTO person(imie, nazwisko, email)
             VALUES(?,?,?)'''

   cur = conn.cursor()
   cur.execute(sql, person)
   conn.commit()
   return cur.lastrowid



def add_task(conn, task):
   """
   Create a new task into the tasks table
   :param conn:
   :param tasks:
   :return: task id
   """
   sql =   '''INSERT INTO tasks(persona_id, nazwa, opis, status, start_date, end_date)
            VALUES(?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, task)
   conn.commit()
   return cur.lastrowid

def fetch_all_persons(conn):
    """
    Fetch all rows from the person table
    :param conn: the Connection object
    :return: rows of data
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM person")

    rows = cur.fetchall()

    for row in rows:
        print(row)

    return rows

def update_task(conn, task_id):
    """
    Mark a task as completed in the tasks table
    :param conn: the Connection object
    :param task_id: id of the task to be updated
    :return:
    """
    sql = ''' UPDATE tasks
              SET status = 'completed'
              WHERE id = ?'''
    cur = conn.cursor()
    cur.execute(sql, (task_id,))
    conn.commit()

    

def delete_person(conn, person_id):
    """
    Delete a person from the person table
    :param conn: the Connection object
    :param person_id: id of the person to be deleted
    :return:
    """
    sql = ''' DELETE FROM person WHERE id = ? '''
    cur = conn.cursor()
    cur.execute(sql, (person_id,))
    conn.commit()

def delete_all_persons(conn):
    """
    Delete all persons from the person table
    :param conn: the Connection object
    :return:
    """
    sql = ''' DELETE FROM person '''
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()



conn = create_connection("my_database.db")
if conn is not None:
    #persons = fetch_all_persons(conn)
    #update_task(conn, 1)
    #delete_person(conn, 2)
    #delete_all_persons(conn)


#person = ("Marian", "Paździoch", "mp@kiepscy.pl")
#pr_id = add_person(conn, person)

#task = (pr_id, "zakupy", "pojechać do lidla", "incomplete", '2023-02-17', '2023-08-20')
#task_id = add_task(conn, task)



#print(pr_id, task_id)
#conn.commit()
#conn.close()
