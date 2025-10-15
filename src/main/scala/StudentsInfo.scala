import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.SparkSession
import java.time.LocalDate

object StudentsInfo {
  def main(args: Array[String]): Unit = {

    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)

    val spark = SparkSession.builder()
      .appName("Students Data Information")
      .master("local[*]")
      .getOrCreate()

    val sc = spark.sparkContext
    val rdd = sc.textFile("data/students.csv")
    val header = rdd.first()
    val dataRDD = rdd.filter(_ != header)

    //1- split students into classes by studying status and count the number of students in each class
    val statusCount = dataRDD
      .map(line => line.split(",")(5))
      .map(status => (status, 1))
      .reduceByKey(_ + _)

    println("Number of students by status:")
    statusCount.collect().foreach(println)

    //2- view student first name, last name, and GPA for students who have graduated
    val graduated = dataRDD
      .map(line => line.split(","))
      .filter(arr => arr(5) == "graduated")
      .map(arr => (arr(1), arr(2), arr(4)))

    println("\nGraduated students:")
    graduated.collect().foreach(println)

    //3- Calculate the mean of GPAs for students who are studying
    val studyingGPA = dataRDD
      .map(line => line.split(","))
      .filter(arr => arr(5) == "studying")
      .map(arr => arr(4).toDouble)

    val meanGPA = studyingGPA.sum() / studyingGPA.count()
    println(s"\nMean GPA for studying students: $meanGPA")

    //4- Sort students by GPA in descending order
    val sortedByGPA = dataRDD
      .map(line => line.split(","))
      .map(arr => (arr(4).toDouble, arr))
      .sortByKey(ascending = false)
      .map(_._2)

    println("\nStudents sorted by GPA:")
    sortedByGPA.collect().foreach(row => println(row.mkString(",")))

    //5- Add a new column 'age' calculated from birthdate
    val withAge = dataRDD
      .map(line => line.split(","))
      .map(arr => {
        val parts = arr(3).split("-")
        val birthYear = parts(0).toInt
        val currentYear = LocalDate.now().getYear
        val age = currentYear - birthYear
        (arr :+ age.toString).mkString(",")
      })

    println("\nStudents with age:")
    withAge.collect().foreach(println)

    //6- show only the students who have a GPA greater than 3.7
    val highGPA = dataRDD
      .map(line => line.split(","))
      .filter(arr => arr(4).toDouble > 3.7)

    println("\nStudents with GPA > 3.7:")
    highGPA.collect().foreach(row => println(row.mkString(",")))

    spark.stop()
  }
}
