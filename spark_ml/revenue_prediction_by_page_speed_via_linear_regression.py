from pyspark.sql import SparkSession, functions as func, types, DataFrame
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression


def convert_to_feature_vector(load_speed):
    """
    Convert load speed value to a vector of feature containing only speed value.
    :param load_speed:
    :return:
    """
    return Vectors.dense(round(float(load_speed), 2))


def extract_dataset(spark: SparkSession) -> DataFrame:
    """
    Extract dataset from file and
    create a dataframe from dataset compatible to linear regression algorithm
    :param spark:
    :return:
    """
    page_schema = types.StructType([
        types.StructField('revenue', types.FloatType(), False),
        types.StructField('load_speed', types.FloatType(), False),
    ])

    page_dataset_df = spark.read.schema(page_schema).csv('dataset/regression.txt', sep=',')

    return page_dataset_df


def transform_dataset_to_label_feature_form(dataset_df):
    """
    Transform dataset to (label, features) form compatible with regression models training:
    First column (label): The value we want to predict.
    Second column (features): It is dense vector of features base on which label is to be predicted.
    :param dataset_df:
    :return:
    """

    # 1) Transform using user defined function
    # convert_to_feature_vector_udf = func.udf(convert_to_feature_vector, VectorUDT())
    # page_dataset_df = page_dataset_df.withColumn('features', convert_to_feature_vector_udf(func.col('load_speed'))). \
    #                   select(func.col('revenue').alias('label'), func.col('features')).cache()

    # 2) Transform using VectorAssembler
    vector_assembler = VectorAssembler().setInputCols(['load_speed']).setOutputCol('features')
    return vector_assembler.transform(page_dataset_df).\
        select(func.col('revenue').alias('actual_revenue'), 'features')


def predict_revenue_by_linear_regression(page_dataset_df: DataFrame):
    """
    Predict revenue gained for every website page based on its load speed feature.
    :param page_dataset_df:
    :return:
    """

    page_dataset_df = transform_dataset_to_label_feature_form(page_dataset_df)

    # Divide dataset to two datasets for training and testing prediction model.
    train_test_datasets = page_dataset_df.randomSplit([0.5, 0.5])
    train_dataset_df = train_test_datasets[0]
    test_dataset_df = train_test_datasets[1]

    # Create a linear regression object with tuned hyper params.
    lr = LinearRegression().setMaxIter(5).setRegParam(0.3).setElasticNetParam(0.8).setLabelCol('actual_revenue')

    # Generate a linear regression model by training it with train dataset.
    lr_model = lr.fit(train_dataset_df)

    # Create predictions for testing dataset.
    # It contains row of "label", "feature' and prediction.
    predictions_df = lr_model.transform(test_dataset_df).cache()

    return predictions_df.select('actual_revenue', func.round(func.col('prediction'), 2).alias('predicted_revenue')).\
        orderBy(func.desc('actual_revenue')).cache()


if __name__ == '__main__':
    spark = SparkSession.builder.appName('revenue_prediction').master('local[*]').getOrCreate()

    page_dataset_df = extract_dataset(spark)

    print('Dataframe compatible with linear regression:')
    page_dataset_df.show()

    revenue_predictions = predict_revenue_by_linear_regression(page_dataset_df)

    print('Revenue predictions for testing dataset:')
    revenue_predictions.show()

    spark.stop()
