import psycopg2
# Connect to an existing database


def run():
    conn = psycopg2.connect("dbname=tsdb user=postgres password=password host=localhost port=5432")
    cursor = conn.cursor()
    q = """
    DELETE FROM tsdb_erkennung;
    """
    cursor.execute(q)
    conn.commit()
    conn.close()