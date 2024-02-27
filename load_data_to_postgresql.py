import logging.config

logging.config.fileConfig('Properties/configuration/logging.config')

loggers = logging.getLogger('Load_postgresql')



#Connection details
PSQL_USRRNAME = "postgres"
PSQL_PASSWORD = "code"

URL = "jdbc:postgresql://localhost:5432/FastAPIDB"
properties = {"user": "postgres","password": "code", "driver": "org.postgresql.Driver"}

def load_data_to_postgresql(df,table_name):
    try:
       #
       df.write.jdbc(url=URL, table=table_name, mode="overwrite",properties = properties )
    except Exception as e:
        loggers.error("An error occured while loading data into the postgresQl ....", str(e))
        raise
    else:
        loggers.warning("Data_report1 succesfully executed..., go frwd")

