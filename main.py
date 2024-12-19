import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, regexp_replace, lit, expr, count
import matplotlib.pyplot as plt
from matplotlib import rcParams

# 设置中文字体
rcParams['font.sans-serif'] = ['SimHei']  # 设置支持中文的字体
rcParams['axes.unicode_minus'] = False  # 确保负号显示正常

# 初始化 Spark 会话
spark = SparkSession.builder.appName("UsedCarAnalysis").getOrCreate()

# 加载数据
file_path = 'guolv.csv'
data = spark.read.csv(file_path, header=True, inferSchema=True)

# 清理并转换字段
data = data.withColumn("nianfen", regexp_replace(col("nianfen"), "年", "").cast("int"))
data = data.withColumn("shoujia", regexp_replace(col("shoujia"), "万", "").cast("float"))
data = data.withColumn("yuanjia", regexp_replace(col("yuanjia"), "万", "").cast("float"))
data = data.withColumn("licheng", regexp_replace(col("licheng"), "万公里", "").cast("float"))

# 计算使用年限
current_year = 2024  # 假设当前年份为 2024
data = data.withColumn("usage_years", (current_year - col("nianfen")).cast("int"))

# 计算掉价
data = data.withColumn("price_drop", (col("yuanjia") - col("shoujia")).cast("float"))

# 计算每年掉价
data = data.withColumn("annual_price_drop", (col("price_drop") / col("usage_years")).cast("float"))


# 查看数据结构
data.printSchema()
data.show(5)

# 1. 品牌与平均价格的关系柱状图
brand_avg_price = (
    data.groupBy("brand")
    .agg(round(avg("shoujia"), 2).alias("avg_price"))
    .orderBy("avg_price", ascending=False)
)

brand_avg_price_pd = brand_avg_price.toPandas()

plt.figure(figsize=(20, 10))  # 提高分辨率
plt.bar(brand_avg_price_pd["brand"], brand_avg_price_pd["avg_price"], color="skyblue")
plt.xticks(rotation=45, ha="right")
plt.xlabel("品牌")
plt.ylabel("平均价格 (万元)")
plt.title("品牌与平均价格的关系")
plt.tight_layout()

# 保存图像
plt.savefig("brand_avg_price.png")  # 保存为 PNG 文件
plt.close()

# 2. 使用年限与平均交易价格的关系折线图
usage_price = (
    data.groupBy("usage_years")
    .agg(round(avg("shoujia"), 2).alias("avg_price"))
    .orderBy("usage_years")
)

usage_price_pd = usage_price.toPandas()

plt.figure(figsize=(20, 10))  # 提高分辨率
plt.plot(usage_price_pd["usage_years"], usage_price_pd["avg_price"], marker="o", color="orange")
plt.xlabel("使用年限 (年)")
plt.ylabel("平均交易价格 (万元)")
plt.title("使用年限与平均交易价格的关系")
plt.grid()
plt.tight_layout()

# 保存图像
plt.savefig("usage_years_avg_price.png")  # 保存为 PNG 文件
plt.close()

# 3. 不同品牌的年度保值率柱状图
annual_resale_rate = (
    data.groupBy("brand")
    .agg(round((avg("shoujia") / avg("yuanjia")) * 100 / avg("usage_years"), 2).alias("annual_resale_rate"))
    .orderBy("annual_resale_rate", ascending=False)
)

annual_resale_rate_pd = annual_resale_rate.toPandas()

# 过滤掉 NaN 或无效数据
annual_resale_rate_pd = annual_resale_rate_pd.dropna(subset=["annual_resale_rate"])

plt.figure(figsize=(20, 10))  # 提高分辨率
plt.bar(annual_resale_rate_pd["brand"], annual_resale_rate_pd["annual_resale_rate"], color="purple")
plt.xticks(rotation=45, ha="right")
plt.xlabel("品牌")
plt.ylabel("年度保值率 (%)")
plt.title("不同品牌的年度保值率")
plt.tight_layout()

# 保存图像
plt.savefig("annual_resale_rate.png")  # 保存为 PNG 文件
plt.close()

# 4. 计算每1万公里的掉价值，并添加为新的一列数据
# 这里将计算的每1万公里掉价值添加到最终的数据中
data = data.withColumn(
    "drop_per_10k_km", (col("price_drop") / col("licheng")).cast("float")
)


# 导出最终的表格到 Excel
output_path = "processed_cars.xlsx"
final_data_pd = data.toPandas()
final_data_pd.to_excel(output_path, index=False)



print(f"数据已导出到 {output_path}")

# 关闭 Spark 会话
spark.stop()
