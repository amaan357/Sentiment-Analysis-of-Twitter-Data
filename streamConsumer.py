from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import preprocessor as p
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer 
nltk.download('vader_lexicon')

# Create a local StreamingContext with two working thread and batch interval of 1 second.
sc = SparkContext(appName="PythonSparkStreamingKafka")
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 60) # 1 - batchDuration 
sid = SentimentIntensityAnalyzer()

def analyse(sentence):
	ss = sid.polarity_scores(sentence)
	if ss["compound"] < 0:
		return "negative"
	elif ss["compound"] > 0:
		return "positive"
	elif ss["compound"] == 0:
		return "neutral"

# connect to kafka
kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', 'spark-streaming', {'twitter': 1})

parsed = kafkaStream.map(lambda x:p.clean(x[1].encode(errors='ignore').decode('utf-8')))
parsed = parsed.map(lambda x:x+"\n"+analyse(x)+"\n").pprint()


ssc.start()
ssc.awaitTermination()




