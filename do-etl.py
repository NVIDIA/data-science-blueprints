#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import sys
import re

app_name = "churn-etl"
default_input_files = dict(
    billing="billing_events", 
    account_features="customer_account_features", 
    internet_features="customer_internet_features", 
    meta="customer_meta", 
    phone_features="customer_phone_features"
)

default_output_file = "churn-etl"
default_output_prefix = ""
default_input_prefix = ""
default_output_mode = "overwrite"
default_output_kind = "parquet"
default_input_kind = "parquet"

parser = parser = argparse.ArgumentParser()
parser.add_argument('--output-file', help='location for denormalized output data (default="%s")' % default_output_file, default=default_output_file)
parser.add_argument('--output-mode', help='Spark data source output mode for the result (default: overwrite)', default=default_output_mode)
parser.add_argument('--input-prefix', help='text to prepend to every input file path (e.g., "hdfs:///churn-raw-data/"; the default is empty)', default=default_input_prefix)
parser.add_argument('--output-prefix', help='text to prepend to every output file (e.g., "hdfs:///churn-data-etl/"; the default is empty)', default=default_output_prefix)
parser.add_argument('--output-kind', help='output Spark data source type for the result (default: parquet)', default=default_output_kind)
parser.add_argument('--input-kind', help='Spark data source type for the input (default: parquet)', default=default_input_kind)
parser.add_argument('--report-file', help='location in which to store an output report', default='report.txt')

if __name__ == '__main__':
    import pyspark
    import os
    
    args = parser.parse_args()

    session = pyspark.sql.SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

    session

    import churn.etl

    input_files = {k: "%s%s" % (args.input_prefix, v) for k, v in default_input_files.items()}

    churn.etl.register_options(
        app_name = app_name,
        input_files = input_files,
        output_prefix = args.output_prefix,
        output_mode = args.output_mode,
        output_kind = args.output_kind,
        input_kind = args.input_kind,
        output_file = "%s%s" % (args.output_prefix, args.output_file)
    )

    from churn.etl import read_df
    billing_events = read_df(session, input_files["billing"])

    from churn.etl import join_billing_data
    customer_billing = join_billing_data(billing_events)

    from churn.etl import customers as get_customers
    customers = get_customers()

    phone_features = read_df(session, input_files["phone_features"])

    from churn.etl import join_phone_features
    customer_phone_features = join_phone_features(phone_features)

    internet_features = read_df(session, input_files["internet_features"])
    from churn.etl import join_internet_features
    customer_internet_features = join_internet_features(internet_features)

    account_features = read_df(session, input_files["account_features"])

    from churn.etl import join_account_features
    customer_account_features = join_account_features(account_features)

    account_meta = read_df(session, input_files["meta"])

    from churn.etl import process_account_meta
    customer_account_meta = process_account_meta(account_meta)

    from churn.etl import chained_join

    wide_data = chained_join(
        "customerID",
        customers,
        [
            customer_billing,
            customer_phone_features,
            customer_internet_features,
            customer_account_features,
            customer_account_meta
        ]
    ).select(
        "customerID", 
        "gender", 
        "SeniorCitizen", 
        "Partner", 
        "Dependents", 
        "tenure", 
        "PhoneService", 
        "MultipleLines", 
        "InternetService", 
        "OnlineSecurity", 
        "OnlineBackup", 
        "DeviceProtection", 
        "TechSupport", 
        "StreamingTV", 
        "StreamingMovies", 
        "Contract", 
        "PaperlessBilling", 
        "PaymentMethod", 
        "MonthlyCharges", 
        "TotalCharges", 
        "Churn"
    )

    from churn.etl import write_df
    import timeit
    
    output_file = churn.etl.options['output_file']

    elapsed_time = timeit.timeit(lambda: write_df(wide_data, output_file), number=1)

    print("completed ETL pipeline in %f seconds" % elapsed_time)

    records = session.read.parquet(output_file).count()

    first_line = 'Generated %d records in %f seconds; configuration follows:\n\n' % (records, elapsed_time)
    print(first_line)

    with open(args.report_file, "w") as report:
        report.write(first_line + "\n")
        for conf in session.sparkContext.getConf().getAll():
            report.write(str(conf) + "\n")
            print(conf)

    session.stop()