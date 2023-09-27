from pyspark.sql import SparkSession, DataFrame, Row

spark = SparkSession.builder.getOrCreate()

categoryDataFrame = spark.createDataFrame([
    Row(id=0, name="Milk Product"),
    Row(id=1, name="Vegetables"),
    Row(id=2, name="Fruit"),
    Row(id=3, name="Liquid"),
    Row(id=4, name="With sugar"),
    Row(id=5, name="Wheat"),
    Row(id=6, name="Technique"),
    Row(id=7, name="Clothes")
])

productDataFrame = spark.createDataFrame([
    Row(id=0, name="Cheese"),
    Row(id=1, name="Milk"),
    Row(id=2, name="Carrots"),
    Row(id=3, name="Apple"),
    Row(id=4, name="Water"),
    Row(id=5, name="Cola"),
    Row(id=6, name="Bread"),
    Row(id=7, name="Smartphone"),
    Row(id=8, name="Laptop"),
    Row(id=9, name="T-shirt"),
    Row(id=10, name="Knife")
])

relationsDataFrame = spark.createDataFrame([
    Row(productID=0, categoryID=0),
    Row(productID=1, categoryID=0),
    Row(productID=1, categoryID=3),
    Row(productID=2, categoryID=1),
    Row(productID=3, categoryID=2),
    Row(productID=4, categoryID=3),
    Row(productID=5, categoryID=3),
    Row(productID=5, categoryID=4),
    Row(productID=6, categoryID=5),
    Row(productID=7, categoryID=6),
    Row(productID=8, categoryID=6),
    Row(productID=9, categoryID=7),
])


def joinProductsAndCategories(products: DataFrame, relations: DataFrame, categories: DataFrame):
    return products.join(
        relations, products.id == relations.productID, 'outer'
    ).join(
        categories, categories.id == relations.categoryID, 'outer'
    ).select(
        products.name.alias('Product'), categories.name.alias('Category')
    )


result = joinProductsAndCategories(products=productDataFrame, relations=relationsDataFrame,
                                   categories=categoryDataFrame)
result.show()
