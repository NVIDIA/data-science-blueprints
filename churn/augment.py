import datetime
import os

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F
from collections import defaultdict

options = defaultdict(lambda: None)

now = datetime.datetime.now(datetime.timezone.utc)


def register_options(**kwargs):
    global options
    for k, v in kwargs.items():
        options[k] = v

def load_supplied_data(session, input_file):
    fields = [
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
        "Churn",
    ]
    double_fields = set(["tenure", "MonthlyCharges", "TotalCharges"])

    schema = pyspark.sql.types.StructType(
        [
            pyspark.sql.types.StructField(
                f, DoubleType() if f in double_fields else StringType()
            )
            for f in fields
        ]
    )

    df = session.read.csv(input_file, header=True, schema=schema)
    
    df = df.dropna()
    
    return df

def replicate_df(df, duplicates):
    def str_part(seed=0x5CA1AB1E):
        "generate the string part of a unique ID"
        import random

        r = random.Random(seed)
        from base64 import b64encode

        while True:
            yield "%s" % b64encode(r.getrandbits(48).to_bytes(6, "big"), b"@_").decode(
                "utf-8"
            )

    if duplicates > 1:
        sp = str_part()
        session = df.sql_ctx.sparkSession

        uniques = (
            session.createDataFrame(
                schema=StructType([StructField("u_value", StringType())]),
                data=[dict(u_value=next(sp)) for _ in range(duplicates * 2)],
            )
            .distinct()
            .limit(duplicates)
        )

        uc = uniques.count()
        if uc != duplicates:
            print(
                "warning:  got some rng collision and have %d instead of %d duplicates"
                % (uc, duplicates)
            )

        df = (
            df.crossJoin(uniques.distinct())
            .withColumn("customerID", F.format_string("%s-%s", "customerID", "u_value"))
            .drop("u_value")
        )

    return df

def examine_categoricals(df, columns=None):
    """ Returns (to driver memory) a list of tuples consisting of every unique value 
        for each column in `columns` or for every categorical column in the source 
        data if no columns are specified """
    default_columns = [
        "SeniorCitizen",
        "Partner",
        "Dependents",
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
    ]

    columns = columns or default_columns

    return [(c, [row[0] for row in df.select(c).distinct().rdd.collect()]) for c in columns]

def billing_events(df):
    import datetime

    w = pyspark.sql.Window.orderBy(F.lit("")).partitionBy(df.customerID)

    charges = (
        df.select(
            df.customerID,
            F.lit("Charge").alias("kind"),
            F.explode(
                F.array_repeat(df.TotalCharges / df.tenure, df.tenure.cast("int"))
            ).alias("value"),
        )
        .withColumn("now", F.lit(now))
        .withColumn("month_number", -F.row_number().over(w))
        .withColumn("date", F.expr("add_months(now, month_number)"))
        .drop("now", "month_number")
    )

    serviceStarts = (
        df.select(
            df.customerID,
            F.lit("AccountCreation").alias("kind"),
            F.lit(0.0).alias("value"),
            F.lit(now).alias("now"),
            (-df.tenure - 1).alias("month_number"),
        )
        .withColumn("date", F.expr("add_months(now, month_number)"))
        .drop("now", "month_number")
    )

    serviceTerminations = df.where(df.Churn == "Yes").select(
        df.customerID,
        F.lit("AccountTermination").alias("kind"),
        F.lit(0.0).alias("value"),
        F.add_months(F.lit(now), 0).alias("date"),
    )

    billingEvents = charges.union(serviceStarts).union(serviceTerminations).orderBy("date").withColumn("month", F.substring("date", 0, 7))
    return billingEvents

def write_df(df, name, skip_replication=False, partition_by=None):
    dup_times = options["dup_times"] or 1
    output_prefix = options["output_prefix"] or ""
    output_mode = options["output_mode"] or "overwrite"
    output_kind = options["output_kind"] or "parquet"

    if not skip_replication:
        df = replicate_df(df, dup_times)
    write = df.write
    if partition_by is not None:
        if type(partition_by) == str:
            partition_by = [partition_by]
        write = write.partitionBy(*partition_by)
    name = "%s.%s" % (name, output_kind)
    if output_prefix != "":
        name = "%s-%s" % (output_prefix, name)
    kwargs = {}
    if output_kind == "csv":
        kwargs["header"] = True
    getattr(write.mode(output_mode), output_kind)(name, **kwargs)

def customer_meta(df):
    SENIOR_CUTOFF = 65
    ADULT_CUTOFF = 18
    DAYS_IN_YEAR = 365.25
    EXPONENTIAL_DIST_SCALE = 6.3

    augmented_original = replicate_df(df, options["dup_times"] or 1)

    customerMetaRaw = augmented_original.select(
        "customerID",
        F.lit(now).alias("now"),
        (F.abs(F.hash(augmented_original.customerID)) % 4096 / 4096).alias("choice"),
        "SeniorCitizen",
        "gender",
        "Partner",
        "Dependents",
        "MonthlyCharges",
    )

    customerMetaRaw = customerMetaRaw.withColumn(
        "ageInDays",
        F.floor(
            F.when(
                customerMetaRaw.SeniorCitizen == 0,
                (
                    customerMetaRaw.choice
                    * ((SENIOR_CUTOFF - ADULT_CUTOFF - 1) * DAYS_IN_YEAR)
                )
                + (ADULT_CUTOFF * DAYS_IN_YEAR),
            ).otherwise(
                (SENIOR_CUTOFF * DAYS_IN_YEAR)
                + (
                    DAYS_IN_YEAR
                    * (-F.log1p(-customerMetaRaw.choice) * EXPONENTIAL_DIST_SCALE)
                )
            )
        ).cast("int"),
    )

    customerMetaRaw = customerMetaRaw.withColumn(
        "dateOfBirth", F.expr("date_sub(now, ageInDays)")
    )

    return customerMetaRaw.select(
        "customerID",
        "dateOfBirth",
        "gender",
        "SeniorCitizen",
        "Partner",
        "Dependents",
        "MonthlyCharges",
        "now",
    ).orderBy("customerID")


def phone_features(df):
    phoneService = df.select(
        "customerID", F.lit("PhoneService").alias("feature"), F.lit("Yes").alias("value")
    ).where(df.PhoneService == "Yes")

    multipleLines = df.select(
        "customerID", F.lit("MultipleLines").alias("feature"), F.lit("Yes").alias("value")
    ).where(df.MultipleLines == "Yes")

    return phoneService.union(multipleLines).orderBy("customerID")

def internet_features(df):
    internet_service = df.select(
        "customerID",
        F.lit("InternetService").alias("feature"),
        df.InternetService.alias("value"),
    ).where(df.InternetService != "No")

    customerInternetFeatures = internet_service

    for feature in [
        "InternetService",
        "OnlineSecurity",
        "OnlineBackup",
        "DeviceProtection",
        "TechSupport",
        "StreamingTV",
        "StreamingMovies",
    ]:
        tmpdf = df.select(
            "customerID",
            F.lit(feature).alias("feature"),
            df[feature].alias("value"),
        ).where(df[feature] == "Yes")

        customerInternetFeatures = customerInternetFeatures.union(tmpdf)

    return customerInternetFeatures


def account_features(df):
    session = df.sql_ctx.sparkSession
    accountSchema = pyspark.sql.types.StructType(
        [
            pyspark.sql.types.StructField(f, StringType())
            for f in ["customerID", "feature", "value"]
        ]
    )

    customerAccountFeatures = session.createDataFrame(schema=accountSchema, data=[])

    for feature in ["Contract", "PaperlessBilling", "PaymentMethod"]:
        tmpdf = df.select(
            "customerID",
            F.lit(feature).alias("feature"),
            df[feature].alias("value"),
        ).where(df[feature] != "No")

        customerAccountFeatures = customerAccountFeatures.union(tmpdf)
    
    return customerAccountFeatures


def debug_augmentation(df):
    return (
        df.select("customerID")
        .distinct()
        .select(
            "customerID",
            F.substring("customerID", 0, 10).alias("originalID"),
            F.element_at(F.split("customerID", "-", -1), 3).alias("suffix"),
        )
    )