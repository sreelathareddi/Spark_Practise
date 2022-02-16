package com.sreelatha.spark_example
package com.sreelatha.spark_example

import org.apache.spark.sql.SparkSession

object DataFrame_operations extends App{
  val session = SparkSession.builder().master("local[*]").appName("DataFrame_Operations").getOrCreate();
  val spark=session.sqlContext
  //reading csv file to dataframe
  import org.apache.spark.sql.functions.{count}
  val emp=spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("file:///C://install/ExampleFiles/employees.csv")
    emp.show()
    val empCount=emp.groupBy("JOB_ID").agg(count("EMPLOYEE_ID")).alias("Employees")
    empCount.show()

  val employeeData = Seq(
    Employee(1,"Ranga", 34,"Male",30000, 1),
    Employee(2,"Nishanth",3, "Male",40000, 1),
    Employee(3,"Raja",60,"Male",100000, 2),
    Employee(4,"Vasu",55,"FeMale",200000, 3),
    Employee(5,"Meena",30,"FeMale",15000, 4)
  )
  import spark.implicits._
  val employeeDF = employeeData.toDF()
  employeeDF.show(false)

  val departmentData = Seq(
    Department(1, "Software"),
    Department(2, "Finance"),
    Department(3, "Sales"),
    Department(4, "Hardware")
  )
  val departmentDF = departmentData.toDF().withColumnRenamed("name", "dept_name")
  departmentDF.show(false)

  val joinedEmpDeptDF = employeeDF.join(departmentDF, Seq("id"),"inner")
  joinedEmpDeptDF.show(false)

  joinedEmpDeptDF.write.mode("overwrite").saveAsTable("employee_department")

  val empDeptDF = spark.sql("select * from employee_department")
  empDeptDF.show(false)


  session.close()
}


