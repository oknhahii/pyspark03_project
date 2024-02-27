import os
from time import perf_counter

import  get_env_variables as gev
from create_spark import get_spark_session
from validate import  *
import logging
from ingrest import  load_files,display_df, df_count
import logging.config
from data_processing import *
from handling_emails import handle_email
from get_dataset_info import get_dataset_info
from data_transformation import data_report1, data_report2
from load_data_to_postgresql import  load_data_to_postgresql

logging.config.fileConfig('Properties/configuration/logging.config')

def main():
    global file_format, file_path, header, inferSchema, file_dir2

    try:
        logging.info('calling spark object')
        spark = get_spark_session(gev.envn, gev.appName)
        logging.info('Spark object is created!')


        logging.info('Validating spark object')
        get_current_date(spark)

        handle_email(spark)


        logging.info('Get file info from olap src')
        ap_dataset = get_dataset_info(gev.src_olap)

        logging.info('Load the cities data')
        df_city = load_files(spark=spark, file_dir=ap_dataset[0]['file_path'], file_format=ap_dataset[0]['file_format'], header=ap_dataset[0]['header'],
                                 inferSchema=ap_dataset[0]['inferSchema'])



        display_df(df_city,'city')
        df_count(df_city,'city')

        logging.info('Get the file info from oltp src')
        tp_dataset = get_dataset_info(gev.src_oltp)

        logging.info('Load the presc medicare data')
        df_presc = load_files(spark=spark, file_dir=tp_dataset[0]['file_path'], file_format=tp_dataset[0]['file_format'],
                             header=tp_dataset[0]['header'],
                             inferSchema=tp_dataset[0]['inferSchema'])

        display_df(df_presc, 'Presc_medicare')
        df_count(df_presc, 'Presc_medicare')

        logging.info('Count the null value in the presc dataset! '
                     '')
        checked_df = check_for_nulls(df_presc, 'df_sel')

        display_df(checked_df, 'checked_df')

        logging.info('Cleaning the two dataframe!')
        df_city_sel, df_presc_sel = data_clean(df_city,df_presc)

        display_df(df_city_sel, 'dfcity-sel')
        display_df(df_presc_sel,'dfpresc-sel')
        #
        show_schema(df_presc_sel,'df_presc_sel')
        show_schema(df_city_sel,'df_city_sel')

        logging.info('Showing the report 1!')
        report1 = data_report1(df_city_sel, df_presc_sel)
        display_df(report1, 'report1')

        logging.info('Showing the report 2!')
        report2 = data_report2(df_presc_sel)
        display_df(report2, 'report2')

        logging.info('Loadin to the postgresQl')
        load_data_to_postgresql(report2, 'ranked_presc')
        load_data_to_postgresql(report1, 'presc_of_the_city')
        load_data_to_postgresql(df_city_sel,'df_city_sel')
        load_data_to_postgresql(df_presc_sel, 'df_presc_sel')


    except Exception as e:
        logging.error('An error occurred when calling main() please check the trace =', str(e))


if __name__ == '__main__':
    start_time = perf_counter()
    main()
    end_time = perf_counter()
    print('The time fo running time is ', end_time - start_time)

    logging.info('The app done')

