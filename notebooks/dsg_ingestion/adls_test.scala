// Databricks notebook source
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "9f55953f-abd7-4cc4-90b9-7d7bf0781d50")
spark.conf.set("dfs.adls.oauth2.credential", "p5nFNSXuUGfdD0578ATHP1pYCT/G1WrCl7SuvV6kD6g=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/5ad9f102-0b7a-4af1-a54a-bacefb9230f1/oauth2/token")

// COMMAND ----------



// COMMAND ----------

@run ./abcd

// COMMAND ----------

dbutils.widgets.text("country", "","")
dbutils.widgets.get("country")
val y = getArgument("country","KH")
//print "Param -\'country':"
//println y

dbutils.fs.ls(s"adl://sriraman.azuredatalakestore.net/unilever/dsg/ingestion/sales-data/KH/2018/09")

// COMMAND ----------

val df = spark.read.option("header", "true")
  .option("inferSchema", "true")
.csv(s"adl://sriraman.azuredatalakestore.net/unilever/dsg/ingestion/sales-data/KH/2018/09/")
df.printSchema()
//df.count()
df.distinct().write.mode(SaveMode.Overwrite).format("delta").saveAsTable("unilever_staging.sales_data")
val df1 = spark.read.format("delta").table("unilever_staging.sales_data")
//df1.show()
df1.count()

// COMMAND ----------

//display(spark.sql("DROP TABLE IF EXISTS quickstart_delta"))
//display(spark.sql("DESCRIBE HISTORY quickstart_delta"))
//display(spark.sql("DROP database IF EXISTS unilever_staging"))
//display(spark.sql("create database unilever_staging"))