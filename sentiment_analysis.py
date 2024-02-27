# pip install -q transformers
import nltk
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('vader_lexicon')
from nltk.sentiment import SentimentIntensityAnalyzer
sia = SentimentIntensityAnalyzer()



def get_sentiment_analysis(rdd):
    # sen = rdd.reduce(lambda x, y: x+" "+y)
    lines = rdd.collect()

    neg = 0
    neu = 0
    compound = 0
    for line in lines:
        scores = sia.polarity_scores(line)
        if scores is not None:
            neg += scores['neg']
            neu += scores['neu']
            compound += scores['compound']

    if compound >= neu and compound >=neg:
        return 'compound'
    elif compound < neu and neu >neg:
        return 'neu'
    else:
        return 'neg'
