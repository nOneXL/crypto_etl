from crypto_etl import cryptoDataEtl
from os import environ

if __name__ == '__main__':
    cryptoDataEtl(environ["DB"], environ["HOST"], environ["PORT"], environ["USERNAME"], environ["PASSWORD"])