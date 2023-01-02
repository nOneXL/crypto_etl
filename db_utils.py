import psycopg2
from sqlalchemy import create_engine


# ENGINE = create_engine("postgresql+psycopg2://{0}:{1}@{2}/{3}".format(
#     USERNAME,
#     PASSWORD,
#     HOST,
#     DB
# ))


schemaName = "crypto"
tableName = "currency"


def connectToDb(db, host, port, username, password):
    conn = psycopg2.connect(database=db,
                            user=username,
                            password=password,
                            host=host,
                            port=port)

    return conn


def createCryptoSchema(db, host, port, username, password):
    conn = connectToDb(db, host, port, username, password)
    cur = conn.cursor()

    query = "CREATE SCHEMA IF NOT EXISTS {0}".format(schemaName)
    cur.execute(query)
    conn.commit()
    print("Schema", schemaName, "exists or created!")

    cur.close()
    conn.close()


def createCryptoCurrencyTable(intColumns, longIntColumns, stringColumns, dateColumns, floatColumns,
                              db, host, port, username, password):
    conn = connectToDb(db, host, port, username, password)
    cur = conn.cursor()

    colTypeList = []

    for intCol in intColumns:
        colTypeList.append("{0} INT".format(intCol))

    for longintCol in longIntColumns:
        colTypeList.append("{0} BIGINT".format(longintCol))

    for stringCol in stringColumns:
        colTypeList.append("{0} VARCHAR".format(stringCol))

    for dateCol in dateColumns:
        colTypeList.append("{0} TIMESTAMP".format(dateCol))

    for floatCol in floatColumns:
        colTypeList.append("{0} DECIMAL".format(floatCol))

    colTypeString = ", ".join(colTypeList)

    query = "CREATE TABLE IF NOT EXISTS {0}.{1}({2});".format(schemaName, tableName, colTypeString)
    cur.execute(query)
    conn.commit()
    print("Table", tableName, "exists or created!")

    cur.close()
    conn.close()


def insertValueToCurrencyTable(columns, data, db, host, port, username, password):
    conn = connectToDb(db, host, port, username, password)
    cur = conn.cursor()

    stringColumns = ", ".join(columns)
    query = "INSERT INTO {0}.{1}({2}) VALUES({3});".format(schemaName, tableName,
                                                           stringColumns, ", ".join(["%s"] * len(data)))
    cur.execute(query, data)
    conn.commit()
    print("New row inserted to {0} table".format(tableName))

    cur.close()
    conn.close()


def insertValuesToCurrencyTable(df, db, host, port, username, password):
    for index, row in df.iterrows():
        insertValueToCurrencyTable(list(row.index), list(row.values), db, host, port, username, password)