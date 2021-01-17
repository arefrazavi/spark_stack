from pyspark.sql import SparkSession, functions as func, DataFrame
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import VectorAssembler


def extract_dataset(spark: SparkSession) -> DataFrame:
    real_estate_dataset_df = spark.read.csv('dataset/realestate.csv', sep=',', inferSchema=True, header=True)

    return real_estate_dataset_df

def transform_dataset_to_label_feature_form(dataset_df):
    """
    Transform dataset to (label, features) form compatible with regression models training:
    First column (label): The value we want to predict.
    Second column (features): It is dense vector of features base on which label is to be predicted.
    :param dataset_df:
    :return:
    """

    # VectorAssembler is a transformer that combines a given list of columns into a single vector column.
    vector_assembler = VectorAssembler().setInputCols(['HouseAge', 'DistanceToMRT', 'NumberConvenienceStores']). \
        setOutputCol('features')
    return vector_assembler.transform(dataset_df). \
        select(func.col('PriceOfUnitArea').alias('actual_price'), 'features')


def predict_price_of_unit_area_by_decision_tree(real_estate_dataset_df: DataFrame):
    """
    Predict the price per unit area based on house age, distance to MRT (public transportation) and number of convenience stores,
    using decision tree regression.
    :param real_estate_dataset_df:
    :return:
    """

    real_estate_dataset_df = transform_dataset_to_label_feature_form(real_estate_dataset_df)

    train_test_datasets = real_estate_dataset_df.randomSplit([0.5, 0.5])
    train_dataset = train_test_datasets[0]
    test_dataset = train_test_datasets[1]

    # setLabelCol, setFeatureCol: Change column name for "label" and "features" columns.
    decision_tree_regressor = DecisionTreeRegressor().setLabelCol('actual_price')
    model = decision_tree_regressor.fit(train_dataset)

    # Create predictions for testing dataset.
    predictions = model.transform(test_dataset).\
        select('actual_price', func.round(func.col('prediction'), 2).alias('predicted_price')).\
        orderBy(func.desc('actual_price')).cache()

    return predictions


if __name__ == "__main__":
    spark = SparkSession.builder.appName('real_estate_prediction').getOrCreate()

    real_estate_dataset_df = extract_dataset(spark)

    price_predictions = predict_price_of_unit_area_by_decision_tree(real_estate_dataset_df)

    print('Predictions for unit area price of houses:')
    price_predictions.show()

    spark.stop()
