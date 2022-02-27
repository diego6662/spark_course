from pyspark import SparkContext, SparkConf
import re


conf = (SparkConf()
.setMaster('local')
.setAppName('WordCount'))
sc = SparkContext(conf = conf)

def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

def get_data(filename):
    rdd = sc.textFile(filename)
    return rdd

def main():
    filename = './count_words/data/Book'
    df = get_data(filename)
    # words = df.flatMap(lambda x: x.split())# 'hello world' -> ['hello', 'world']
    words = df.flatMap(normalize_words)
    word_counts = words.countByValue()

    for word, count in word_counts.items():
        cleaned_word = word.encode('ascii', 'ignore')
        if cleaned_word:
            print(cleaned_word, count)

if __name__ == '__main__':
    main()