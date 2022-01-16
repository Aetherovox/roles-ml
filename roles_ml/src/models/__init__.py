from sklearn.feature_extraction.text import CountVectorizer, TfidfTransformer
from sklearn.naive_bayes import MultinomialNB
import json
import pandas as pd
from S3Processor import S3Processor




scrubs = {
    "weblinks"  : r"\bhttps\S+|\bwww\S+",
    "locs"      : r"\bLocation.{,20}",
    "salaries"  : r"\bSalary|\bSALARY|£[^A-Za-z]+\.",
    "benefits"  : r"\bBenefits.+\.|\bbenefits.+",
    "html tags" : r"html",
    "gunk"      : r"\uf0a7|\xa0|\b'{0,1}ll\b",
    "footnotes" : r"Additional Information.+$|Eligible.+\.",
    "Filler1"    : r"[rR]ole|Job (Description|Reference|Type|Title)|About Us",
    "Filler2"   : r"\bdays\b|\bwork\S{0,}",
    "Filler3"   : r"\bteam\b|\b[eE]xperience\b|([sS]e|[jJ]u)nior",
    "Filler4"   : r"\bskills\b|\blooking\b|\bbusiness\b|\bjoin\b|\bopportunity\b",
    "Filler5"   : r"\bnew\b|Apply Now",
    "HR Bullshit": "Growth Mindset|Team Player"
  }

roles = {


    "Cloud Engineer": r"\baws\b|\bAWS\b|[aA]zure|[cC]loud ([aA]rchitect|[eE]ngineer)|Data.+Storage|[mM]odel{1,2}er|[dD]ata\s+[sS]olution|Migration",
    "Data Engineer": r"[dD]ata.+[eE]ngineer\S{0,}|\bETL\b|\bBIG DATA\b|Big Data|Hadoop|Cloudera|\bSpark\b",
    "DB or Systems Admin": r"[aA]dmin.{0,1}|\b(IT|Tech) Support|[sS]ervice [dD]esk",
    "Data Scientist" : r"[dD]ata [sS]cien\S+|Machine Learn.+|M[lL]",
    "Software Engineer": r"[sS]oftware|\b[aA]pp\b|([bB]ack|[fF]ront)end|[fF]ull [sS]tack|[kK]afka|(\bC[+#]{0,2}|Ruby|Swift|Java|Python|Scala|Perl|Rust|Julia|\bR\b) (Developer|Engineer)",
    "Analyst" : r"[aA]nalys\S+|[iI]nsight|[rR]esearch|[aA]ssistant|\bBI\b" ,

    "Management": r"\bProject Manage+.|[dD]elivery Lead|[mM]anage(r|ment)|\b[hH]ead of|[dD]irector",
    "N/A" : "9999999999"

}


class Model:
    def __init__(self):
        self.s3 = S3Processor()
        self.s3.download_file("machine_learning/training_data.csv")
        self.scrub_pattern = self.get_regex(scrubs)
        self.roles_dict = self.get_regex(roles)[0]
        self.roles_pattern = self.get_regex(roles)[1]
        # TODO: read job file in as dataframe
        self.training_file = self.config()["training_file"]
        self.output_file = self.config()["outfile"]
        print('Training_file is: {}'.format(self.training_file))

    @staticmethod
    def config():
        with open('config.json') as conf:
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
