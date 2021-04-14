package covid_package

import org.apache.spark.sql.SparkSession

object Covid {
  def main(args: Array[String]): Unit = {

//    create spark session
    val spark = SparkSession.builder()
      .appName("covid-19")
      .master("local")
      .getOrCreate()

      val covidDataSet = spark.read
        .option("header", "true")
        .option("inferSchema", "true")              //load schema with datatypes according to its own understanding
        .csv("src//test//Datasets//covid-data.csv")

//    covidDataSet.show(10)
//    covidDataSet.printSchema()

    covidDataSet.createTempView("covidTableView")

    val covidFilteredResult = spark
      .sql("select count(*) as count,max(total_cases) as cases, location " +
        "from covidTableView " +
        "group by location " +
        "order by location")

//    covidFilteredResult.show(10)


    val locationNames = covidFilteredResult.select("location")
    locationNames.show()
    val locList = locationNames.select("location").rdd.map(r => r(0).asInstanceOf[String]).collect()



    covidFilteredResult.createTempView("maxCases")
    val maxcase = spark.sql("select * from maxCases PIVOT (count(*) FOR location IN ('India' as country))")

    maxcase.show(1000)

    println("hello world")


  }
}
