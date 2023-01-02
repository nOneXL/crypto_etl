import requests
import numpy as np
import pandas as pd
from db_utils import createCryptoSchema, createCryptoCurrencyTable, insertValuesToCurrencyTable
from data_utils import checkIfStringIsFloat


def getApiData(url):
    res = requests.get(url)

    if res.ok:
        return res.json()
    return None


def checkIfDataIsValid(df):
    # Check for emtpy list
    if df.empty:
        print("Dataframe is empty. Finishing execution")
        return False
    return True


def returnIsFloatSeries(series):
    isFloatSeries = series.map(lambda x: checkIfStringIsFloat(x))
    return isFloatSeries


def returnIsIntSeries(series):
    isFloatSeries = series.map(lambda x: isinstance(x, int))
    return isFloatSeries


def boolFilterDefectiveFloatSeries(df):
    isFloatDf = df.apply(returnIsFloatSeries)
    minNumberInRowDf = isFloatDf.apply(np.min, axis=1)
    return minNumberInRowDf


def boolFilterDefectiveIntSeries(df):
    isIntDf = df.apply(returnIsIntSeries)
    minNumberInRowDf = isIntDf.apply(np.min, axis=1)
    return minNumberInRowDf


def filterDefectiveDataframeData(df, columns, filter_function):
    boolIndexFilterDf = filter_function(df[columns])
    filteredDf = df[boolIndexFilterDf]
    return filteredDf


def transformStringTypeToFloat(df, floatColumns):
    typeTransformDict = {floatCol: "float64" for floatCol in floatColumns}
    return df.astype(typeTransformDict)


def transformEpochSeriesToDate(epochSeries):
    return pd.to_datetime(epochSeries, unit='ms')


def transformTypes(df, floatColumns, dateColumns):
    stringToFloat = transformStringTypeToFloat(df, floatColumns)

    for dateCol in dateColumns:
        stringToFloat[dateCol] = stringToFloat[dateCol].apply(transformEpochSeriesToDate)
    return stringToFloat


def cryptoDataEtl(db, host, port, username, password):
    stringColumns = ["symbol"]
    floatColumns = ["priceChange", "priceChangePercent", "weightedAvgPrice", "prevClosePrice",
                    "lastPrice", "lastQty", "bidPrice", "bidQty", "askPrice", "askQty", "openPrice",
                    "highPrice", "lowPrice", "volume", "quoteVolume"]
    dateColumns = ["openTime", "closeTime"]
    intColumns = ["count"]
    longIntColumns = ["firstId", "lastId"]
    crypto_api = "https://api2.binance.com/api/v3/ticker/24hr"

    # Extract
    cryptoJson = getApiData(crypto_api)

    # Transform
    if cryptoJson:
        cryptoDf = pd.DataFrame.from_dict(cryptoJson)
        if checkIfDataIsValid(cryptoDf):
            filterFloatDf = filterDefectiveDataframeData(cryptoDf, floatColumns, boolFilterDefectiveFloatSeries)
            filterIntDf = filterDefectiveDataframeData(filterFloatDf,
                                                       intColumns + longIntColumns, boolFilterDefectiveIntSeries)
            transformDf = transformTypes(filterIntDf, floatColumns, dateColumns)

            # Load
            createCryptoSchema(db, host, port, username, password)
            createCryptoCurrencyTable(intColumns, longIntColumns, stringColumns, dateColumns, floatColumns,
                                      db, host, port, username, password)
            insertValuesToCurrencyTable(transformDf, db, host, port, username, password)
