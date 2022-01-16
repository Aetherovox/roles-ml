from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
import json
import pandas as pd
from config.regexes import scrubs,roles


class Model:
    def __init__(self):
        self.scrub_pattern = self.get_regex(scrubs)
        self.roles_dict = self.get_regex(roles)[0]
        self.roles_pattern = self.get_regex(roles)[1]
        # TODO: read job file in as dataframe
        self.training_file = self.config()["training_file"]
        self.output_file = self.config()["outfile"]
        print('Training_file is: {}'.format(self.training_file))

    @staticmethod
    def config():
        with open('config/config.json') as conf:
            data = json.load(conf)["files"]
        return data

    # regexes don't agree with json data so I've put them in a python dictionary
    @staticmethod
    def get_regex(regex_type):
        pattern = '999999999999'
        for key in regex_type:
            pattern = pattern + '|' + regex_type[key]
        if regex_type is scrubs:
            return pattern
        elif regex_type is roles:
            return [roles, pattern]





class Classifier(Model):
    def __init__(self):
        Model.__init__(self)
        self.training_data = pd.read_csv(self.training_file)
        self.data = self.training_data['data'].to_list()
        self.targets = self.training_data['labels'].tolist()
        self.vectoriser = CountVectorizer()
        self.x_termcounts = self.vectoriser.fit_transform(self.data)
        self.tf = TfidfTransformer()
        self.train_tf = self.tf.fit_transform(self.x_termcounts)
        self.classifier = MultinomialNB().fit(self.train_tf, self.targets)

    def predict(self, input_data):
        x_input_termcounts = self.vectoriser.transform(input_data)
        x_input_tf = self.tf.transform(x_input_termcounts)
        predicted_categories = self.classifier.predict(x_input_tf)
        return predicted_categories


# ------------------------------------------------------------------
