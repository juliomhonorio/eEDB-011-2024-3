import pyspark

spark = pyspark.sql.SparkSession.builder.appName("banks_raw").getOrCreate()

source_path = 'C:\\Users\\Gabriel\\Documents\\Ingestão de Dados\\Dados\\Bancos\\'
source_file = "EnquadramentoInicia_v2.tsv"

df_target_table = (spark.read.format("csv").option("header", "true")
                                           .option("sep", "\t")
                                           .option("encoding", "ISO-8859-1")
                                           .load(source_path + source_file))

target_path = 'C:\\Users\\Gabriel\\Documents\\Ingestão de Dados\\Tabelas\\1_Raw\\'
target_table = "banks_raw"

df_target_table.write.format("parquet").mode("overwrite").save(target_path + target_table)




