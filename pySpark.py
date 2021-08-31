import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import split

# ------------------------------------ TASK ------------------------------------
# 1- Given this yearly income ranges, <40k, 40-60k, 60-80k, 80-100k and 100k>.
# Generate a report that contains the average loan amount and an average term of the loan in months based on these 5 income ranges.
# Result file should be like “income range, avg amount, avg term”

# 2- In loans which are fully funded and loan amounts greater than $1000,
# what is the fully paid amount rate for every loan grade of the borrowers.
# Result file should be like “credit grade,fully paid amount rate”, eg.“A,%95”
# -------------------------------------------------------------------------------

spark = SparkSession.builder.getOrCreate()
findspark.init()

df = spark.read.format("csv").option("header", "true").load("loan.csv")


def task_1(df):
    print("Task - 1 ")
    df = df.withColumn('termInt', split(df['term'], ' ').getItem(1)).withColumn('termMonths', split(df['term'], ' ').getItem(2))

    # "Income Range : 40k;
    first = df.filter(df.annual_inc == 40000).agg({"loan_amnt": "avg", "termInt": "avg"}) \
        .withColumnRenamed("avg(termInt)", "Average Term (40k)") \
        .withColumnRenamed("avg(loan_amnt)", "Average Amount (40k)")

    # "Income Range : 40k-60k;
    second = df.filter((df.annual_inc > 40000) & (df.annual_inc < 60000)).agg({"loan_amnt": "avg", "termInt": "avg"}) \
        .withColumnRenamed("avg(termInt)", "Average Term (40k-60k)") \
        .withColumnRenamed("avg(loan_amnt)", "Average Amount (40k-60k)")

    # "Income Range : 60k-80k;
    third = df.filter((df.annual_inc > 60000) & (df.annual_inc < 80000)).agg({"loan_amnt": "avg", "termInt": "avg"}) \
        .withColumnRenamed("avg(termInt)", "Average Term (60k-80k)") \
        .withColumnRenamed("avg(loan_amnt)", "Average Amount (60k-80k)")

    # "Income Range : 80k-100k;
    forth = df.filter((df.annual_inc > 80000) & (df.annual_inc < 100000)).agg({"loan_amnt": "avg", "termInt": "avg"}).withColumnRenamed("avg(termInt)",
                                                                                                                                        "Average Term (80k-100k)").withColumnRenamed(
        "avg(loan_amnt)", "Average Amount (80k-100k)")
    # "Income Range : 100k;
    fifth = df.filter(df.annual_inc == 100000).agg({"loan_amnt": "avg", "termInt": "avg"}).withColumnRenamed("avg(termInt)", "Average Term (100k)").withColumnRenamed("avg(loan_amnt)",
                                                                                                                                                                      "Average Amount (100k)")
    allofThem = first.crossJoin(second).crossJoin(third).crossJoin(forth).crossJoin(fifth)
    allofThem.show()


def task_2(df):
    print("Task - 2")
    normal = df.filter((df.loan_amnt > 1000) & (df.funded_amnt == df.loan_amnt))
    fullyPaid = normal.filter(normal.loan_status == 'Fully Paid')
    normal = normal.groupBy("grade").count().withColumnRenamed("count", "normalCount")
    fullyPaid = fullyPaid.groupBy("grade").count().withColumnRenamed("count", "fullyPaidCount")
    join = fullyPaid.join(normal, "grade", how="right").withColumn('fullyOverNormal', (f.col("fullyPaidCount") / f.col('normalCount')) * 100)
    join.show()


# task_1(df)
print()
task_2(df)
