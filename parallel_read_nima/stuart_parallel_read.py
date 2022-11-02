from neo4j import GraphDatabase
from multiprocessing import Pool
import numpy as np
import os
import time
import csv
import sys, getopt


def commandlineOptions(name, argv):

    ## defaults
    batches=8 ## how many batches the id list will be divided into
    processes=8  ## how many threads to run at same time

    try:
        opts, args = getopt.getopt(argv,"p:b:",["batches=","process="])
    except getopt.GetoptError:
        print (name + ' -b <batches> -p <processes_to_use>')
        sys.exit(2)

    for opt, arg in opts:
        if opt in ("-b", "--batches"):
            batches = int(arg)
        elif opt in ("-p", "--processes"):
            processes = int(arg)

    print ('Number Concurrent Processes: ' + str(processes))
    print ('Number Batches: ' + str(batches))

    return batches, processes

def nodes(work_data):
    ## work_data = i, nodes, cypher, username, password, database
    inner_start=time.time()
    read_data(work_data[0],work_data[1],work_data[2],work_data[3],work_data[4],work_data[5],work_data[6])
    inner_end=time.time()
    print("Process: " + str(work_data[0]) + ", finished reading "+ str(len(work_data[1])) + " nodes in " + str((inner_end - inner_start)) + " seconds", flush=True)

def node_pool(processes):
    with Pool(processes) as readPool:
        readPool.map(nodes, work)

def get_ids(tx, query):
    ids = []
    result = tx.run(query)
    for record in result:
        ids.append(record["id"])
    return ids

def get_names(tx, query, nodes):
    ids = []
    result = tx.run(query, batch=nodes)
    for record in result:
        ids.append(record["name"])
    return ids

def read_data(id,nodes,cypher,uri,username,password,database):
    ## Initiate Driver
    driver = GraphDatabase.driver(uri, auth=(username, password))

    with driver.session(database=database) as session:
        names = session.read_transaction(get_names, cypher, nodes)

    ## write to file
    filename="test"+str(id)+".csv"
    csv_file = open(filename, 'w', newline='')
    csv_writer = csv.writer(csv_file)

    for name in names:
        csv_writer.writerow([name])
    csv_file.close()



if __name__ == '__main__':

    batches, processes = commandlineOptions(sys.argv[0],sys.argv[1:])

    ## Neo4j Credentials
    uri = "bolt://localhost:7687"
    username="neo4j"
    password="password" ## change this as needed
    database="neo4j" ## change this as needed

    #### **************************************************************
    ## Get starting node IDs
    #### **************************************************************

    get_ids_cypher="""
        MATCH (n:Personnel) RETURN ID(n) AS id;
    """

    ## Initiate Driver
    driver = GraphDatabase.driver(uri, auth=(username, password))

    ## Get IDs
    with driver.session(database=database) as session:
        ids = session.read_transaction(get_ids, get_ids_cypher)

    print(str(len(ids)) + " read")

    #### **************************************************************
    ## Pass IDs into batches and submit as jobs
    #### **************************************************************

    repeat_cypher="""
        UNWIND $batch as batch
        MATCH (n:Personnel) WHERE ID(n)=batch
        RETURN n.name AS name;
    """

    split=np.array_split(ids,batches)

    work=[]
    for i in range(0, len(split)):
        job=[i, split[i].tolist(), repeat_cypher, uri, username, password, database]
        work.append(job)

    driver.close()

    data_read_start=time.time()
    node_pool(processes)
    data_read_end=time.time()
    print("Finished reading data in " + str((data_read_end - data_read_start)) + " seconds", flush=True)