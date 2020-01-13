package org.awk

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object BostonCrimesMap extends App {

  // аргументы - пути к файлу данных, справочнику, папке для выходного файла
  val crime_file = args(0)
  val offense_codes_file = args(1)
  val output_dir = args(2)

  // master убрать !!!
  val ss = SparkSession.builder().appName("BostonCrimeMap").getOrCreate()
  val sc = ss.sparkContext

  // загружаем данные в df
  val crime_df = ss.read
    .format("csv")
    .option("header", "true")
    .load(crime_file)
    .withColumn("offense_code", col("offense_code").cast(IntegerType))

  // загружаем справочник кодов в df
  val offense_codes_df = ss.read
    .format("csv")
    .option("header", "true")
    .load(offense_codes_file)
    // устраняем дубли
    .groupBy("code").agg(max("name").alias("offense_name"))
    .withColumnRenamed("code", "offense_code")
    .withColumn("offense_type", trim(split(col("offense_name"), "-")(0)))

  // соединяем данные со справочником
  val crime_codes_df = crime_df.join(broadcast(offense_codes_df), "offense_code")

  // 1 группа агрегатов
  val result1_df = crime_codes_df.groupBy("district")
    .agg(
      countDistinct("incident_number").alias("crimes_total"),
      avg(col("lat")).alias("lat"),
      avg(col("long")).alias("lng"))

  // агрегат по месяцам
  // предагрегируем по району и месяцу и сохраняем во временную таблицу
  crime_codes_df.groupBy("district", "year", "month")
    .agg(countDistinct("incident_number").alias("crimes_total"))
    .createOrReplaceTempView("crime_tmp")
  // получаем медиану по месяцам
  val result2_df = ss.sql("select district, percentile_approx(crimes_total, 0.5) as crimes_monthly from crime_tmp group by district")

  // предагрегируем по району и типу,
  // ранжируем по убыванию кол-ва и нумеруем строки,
  // отбираем по 3 строки с макс.кол-вом,
  // ранжируем в группе по номеру строки в обратном порядке,
  // конкатенируем типы в порядке номеров строк,
  // отбираем последнию строку из каждой группы
  val result3_df = crime_codes_df.groupBy("district", "offense_type")
    .agg(countDistinct("incident_number").alias("crimes_total"))
    .withColumn("rn",
      row_number().over(Window.partitionBy(col("district"))
        .orderBy(col("crimes_total").desc)))
    .where(col("rn") <= 3)
    .withColumn("rn_reverse",
      row_number().over(Window.partitionBy(col("district"))
        .orderBy(col("rn").desc)))
    .withColumn("frequent_crime_types",
      concat_ws(
        ", ",
        collect_list("offense_type").over(
          Window.partitionBy("district").orderBy("rn"))))
    .where(col("rn_reverse") === 1)
    .select("district", "frequent_crime_types")

  // result3_df.take(200).foreach(println)

  // соединяем все агрегаты в общий набор
  val result_df = result1_df
    .join(result2_df, "district")
    .join(result3_df, "district")
    .select("district", "crimes_total", "crimes_monthly", "frequent_crime_types", "lat", "lng")

  // выводим в parquet-файл
  result_df.repartition(1).write.format( "parquet").mode("overwrite").save(output_dir)
}
