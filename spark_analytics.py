import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

os.environ['JAVA_HOME'] = r'C:\Program Files\Microsoft\jdk-25.0.2.10-hotspot'
os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-17'

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['spark.python.use.daemon'] = 'false'

print("🚀 Iniciando  Apache Spark... Aguarde.")

# Inicializa o cluster Spar
spark = SparkSession.builder \
    .appName("TradeHub_Analytics_Real") \
    .master("local[*]") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

ARQUIVO_LOG = "market_logs.json"

if not os.path.exists(ARQUIVO_LOG):
    print("❌ Nenhum dado encontrado. Faça uma compra no cliente primeiro.")
else:
    try:
    
        df = spark.read.json(ARQUIVO_LOG)
        
        print("\n" + "="*55)
        print(" 📊 DASHBOARD DE INTELIGÊNCIA DE MERCADO (APACHE SPARK)")
        print("="*55)
        
        # Processamento Massivo Distribuído do Spark:
        total_vendas = df.filter(col("tipo_evento") == "VENDA").count()
        print(f"📈 Total de Skins Vendidas: {total_vendas}")
        
        if "valor" in df.columns:
            receita_row = df.filter(col("tipo_evento") == "VENDA").agg(spark_sum("valor").alias("receita")).collect()[0]
            receita = receita_row["receita"] if receita_row["receita"] is not None else 0
            print(f"💰 Volume Financeiro Total: R$ {receita:,.2f}")
        
        print("\n🏆 Ranking de Popularidade (Skins mais compradas):")
        if "item" in df.columns:
            ranking = df.filter(col("tipo_evento") == "VENDA").groupBy("item").count().orderBy(col("count").desc())
            ranking.show(truncate=False)
            
    except Exception as e:
        print(f"🚨 ERRO DO SPARK: {e}")

spark.stop()