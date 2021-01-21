import argparse
import os
import sys
import re

default_app_name = "augment"
default_input_file = os.path.join("data", "WA_Fn-UseC_-Telco-Customer-Churn-.csv")
default_output_prefix = ""
default_output_mode = "overwrite"
default_output_kind = "parquet"

default_dup_times = 100

parser = parser = argparse.ArgumentParser()
parser.add_argument('--input-file', help='supplied input data (default="%s")' % default_input_file, default=default_input_file)
parser.add_argument('--output-mode', help='Spark data source output mode for the result (default: overwrite)', default=default_output_mode)
parser.add_argument('--output-prefix', help='text to prepend to every output file (e.g., "hdfs:///churn-data/"; the default is empty)', default=default_output_prefix)
parser.add_argument('--output-kind', help='output Spark data source type for the result (default: parquet)', default=default_output_kind)
parser.add_argument('--dup-times', help='scale factor for augmented results (default: 100)', default=default_dup_times, type=int)

if __name__ == '__main__':
    import pyspark
    
    args = parser.parse_args()

    import churn.augment

    churn.augment.register_options(
        app_name = default_app_name,
        input_file = args.input_file,
        output_prefix = args.output_prefix,
        output_mode = args.output_mode,
        output_kind = args.output_kind,
        dup_times = args.dup_times
    )

    session = pyspark.sql.SparkSession.builder.\
        appName(churn.augment.options['app_name']).\
        getOrCreate()
    
    from churn.augment import load_supplied_data

    df = load_supplied_data(session, args.input_file)

    from churn.augment import billing_events
    billingEvents = billing_events(df)

    from churn.augment import customer_meta
    customerMeta = customer_meta(df)

    from churn.augment import phone_features
    customerPhoneFeatures = phone_features(df)

    from churn.augment import internet_features
    customerInternetFeatures = internet_features(df)

    from churn.augment import account_features
    customerAccountFeatures = account_features(df)

    from churn.augment import write_df
    write_df(billingEvents, "billing_events", partition_by="month")
    write_df(customerMeta, "customer_meta", skip_replication=True)
    write_df(customerPhoneFeatures, "customer_phone_features")
    write_df(customerInternetFeatures.orderBy("customerID"), "customer_internet_features")
    write_df(customerAccountFeatures, "customer_account_features")

