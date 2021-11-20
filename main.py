import os
import sys
import psycopg
import datetime

def pushToDatabase(metadata, current_date):

    tuples = metadata[1]
    metadata = metadata[0]

    with psycopg.connect("dbname=northwind user=postgres password=postgres") as conn:
        with conn.cursor() as cur:

            try:
                # we mirror the database lkfans 
                for key in metadata:
                    query = "create table if not exists " + key + " ("
                    for column in metadata[key]:
                        query += column[0] + " " + column[1] + ", "
                    query = query[:-2]
                    query += ")"
                    cur.execute(query)
                    f = open("data/postgres/" + key + "/" + current_date + "/" + key + ".csv", "r")
                    try:
                        while(True):
                            line = f.readline()
                            line = line.replace("None", "null")
                            cur.execute("insert into " + key + " values " + line)
                            cur.commit
                    except Exception as e:
                        print(e)
                            
                    f.close()
            except Exception as e:
                print(e)

def writeFilePostgres(tableName, current_date, content):

    # create a directory for the date stamp if it doesn't exist.
    try:
        absolute_path = os.path.dirname(__file__) + "/data/postgres/" + tableName
        path_table_write = os.path.join(absolute_path, current_date)
        os.mkdir(path_table_write)
    except:
        print("Updating previously existant postgres backup!")
    
    # open and create the folders to write the pseudo-CSV
    tableCSV = open("./data/postgres/" + tableName + "/" + current_date + "/" + tableName + ".csv", "a")

    for line in content:
        tableCSV.write(str(line) + '\n')

    tableCSV.close()

def readFromPostgres(current_date):

    metadata = {}

    # connecting to the SGBD
    # with psycopg.connect("host=0.0.0.0 dbname=northwind user=northwind_user password=thewindisblowing") as conn:
    with psycopg.connect("dbname=testric user=postgres password=postgres") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
            table_names = cur.fetchall()
            # print(table_names)
            tuples = None

            for table in table_names:
                table = table[0]

                # create directory for current table.
                try:
                    absolute_path = os.path.dirname(__file__) + "/data/postgres/"
                    path_table_write = os.path.join(absolute_path, table)
                    os.mkdir(path_table_write)
                except:
                    pass

                cur.execute("select * from " + table)
                tuples = cur.fetchall()

                writeFilePostgres(table, current_date, tuples)

                # we keep track of the meta data for later handling of the retrieved data.
                cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + table + "'")

                metadata[table] = cur.fetchall()

            return (metadata, tuples)

def readFromCSV(current_date):
    # connecting the CSV file to the CSV folder
    try:
        # create a directory for the csv with the date stamp.
        try:
            absolute_path = os.path.dirname(__file__) + "/data/csv/"
            path_csv_write = os.path.join(absolute_path, current_date)
            os.mkdir(path_csv_write)
        except:
            print("Updating previously existant CSV!")
        
        # open and create the folders to write the csv
        order_details = open("./data/order_details.csv", "r")
        order_details_write = open("./data/csv/" + current_date + "/order_details.csv", "a")

        # we copy the file line per line until we reach its end
        try:
            while(True):
                order_details_tuple = order_details.readline()
                order_details_write.write(order_details_tuple)

        except:
            print("Finished writing the CSV file.")

        order_details.close()
        order_details_write.close()
    except:
        print("Error on trying to open 'order_detais.csv'!")


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
    current_date = str(time.year) + "-" + str(time.month) + "-" + str(time.day)

    # step 1
    metadata = readFromPostgres(current_date)
    # readFromCSV(current_date)

    # step 2
    pushToDatabase(metadata, current_date)
    
main()