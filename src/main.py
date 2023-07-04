from dotenv import load_dotenv
from langchain.chat_models import ChatOpenAI
from pyspark_ai import SparkAI

load_dotenv()

llm = ChatOpenAI(model_name='gpt-3.5-turbo', temperature=0)

spark_ai = SparkAI(llm=llm,verbose=True)
spark_ai.activate()

auto_df = spark_ai.create_df("https://www.carpro.com/blog/full-year-2022-national-auto-sales-by-brand")

auto_top_growth_df = auto_df.ai.transform("成長したブランドは？")
auto_top_growth_df.show()