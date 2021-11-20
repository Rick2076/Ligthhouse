import os
import sys
import psycopg
import datetime

def readFromPostgres(current_date):
    # connecting to the SGBD
    with psycopg.connect("dbname=postgres user=postgres password=postgres") as conn:
        with conn.cursor() as cur:
            return
    
def readFromCSV(current_date):
    # connecting the CSV file to the CSV folder
    try:
        order_details = open("./data/order_details.csv", "r")
        order_details_date = open("./data/" + current_date + ".csv", "w")
        while(order_details.)

        order_details.close()
    except:
        print("Error on trying to open 'oder_detais.csv'!")


def main():
    # criamos as pastas para salvar os dados se ainda não houverem.
    try:
        absolute_path = os.path.dirname(__file__) + "/data/"

        path_postgres = os.path.join(absolute_path, "postgres")
        os.mkdir(path_postgres)

        path_csv = os.path.join(absolute_path, "csv")
        os.mkdir(path_csv)
        
    except:
        print("Data directory already existant, skipping data creation step.")

    time = datetime.datetime.now()
    current_date = time.year + "-" + time.month + "-" + time.day

    # step 1
    # readFromPostgres(current_date)
    readFromCSV(current_date)

    # step 2
    # pushToDatabase()

#    try:
#        cur.execute("SELECT * FROM customers")
#        cur.fetchall()
#    cur.execute("SELECT * FROM orders limit 1")

#    for record in cur: 
#        print(record)

#         # Make the changes to the database persistent
#         conn.commit
        
    
main()