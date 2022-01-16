from bs4 import BeautifulSoup
from os import listdir
import json
from nltk.corpus import stopwords
import pandas as pd
import multiprocessing
import re
from config.regexes import scrubs, roles
# todo: lets take a composition based approach


class Scrub:
    def __init__(self):
        self.__roles = roles
        self.__scrub_pattern = self.get_regex(scrubs)
        self.__sw = stopwords.words("english")

    @staticmethod
    def get_regex(regex_type):
        pattern = '999999999999'
        for key in regex_type:
            pattern = pattern + '|' + regex_type[key]
        if regex_type is scrubs:
            return pattern
        elif regex_type is roles:
            return [roles, pattern]


    def find_job_titles(self,data):
        """
        :param data: dataframe
        :return: string
        Finds job title based on regex pattern matching.
        The first match in the string will be returned since it's more likely to reflect the job title if it's closer
            to the beginning of the job description
        """
        x = {}
        for key, value in self.__roles.items():
            try:
                x[re.search(value, data).start()] = key
            except AttributeError:
                pass
        if len(x.values()) == 0:
            return 'N/A'
        minimum = min(x.keys())
        first = x[minimum]
        return first

    def scrub_descriptions(self, data):
        data_without_stopwords = " ".join([word for word in data.split() if word.lower() not in self.__sw])
        return re.sub(self.__scrub_pattern, '', data_without_stopwords)



class JobData:
    def __init__(self):
        self.raw = self.config()["directories"]["raw"]
        self.html_list = [f for f in listdir(self.raw)]
        self.output_file = self.config()["files"]["training_file"]
        self.raw_training_file = self.config()["files"]["kw_job_list"]
        self.scrub = Scrub()


    @staticmethod
    def config():
        with open("config/config.json") as conf:
            x = json.load(conf)
        return x


# TODO: need to look for any of our jobs strings and assign it a job title
    def read_raw(self, html_file):
        output = {}
        output['job_id'] = html_file.split("_")[1]
        output['job_timestamp'] = html_file.split("_")[2].replace('.html', '')

        with open(self.raw + html_file, 'rb') as file:
            soup = BeautifulSoup(file, 'html.parser')
            span = soup.find("span",itemprop="description")

            # strip the data ready to be scrubbed, then scrub it and find job titles from the results
            text_list = span.get_text().split("\n")
            text_stripped = map(lambda x: x.strip(" "),text_list)
            scrubbed = self.scrub.scrub_descriptions("\n".join(text_stripped))
            non_blank = [s for s in scrubbed.split("\n") if s]
            for x in non_blank:
                title_bucket = self.scrub.find_job_titles(x)
                output['job_title'] = title_bucket

            output['job_description'] = scrubbed
        return output

    def scrub_training_data(self):

        df = pd.read_csv(self.raw_training_file)
        df = df.dropna(axis=0,subset=["job_desc"])
        df['job_timestamp'] = 20210301000001
        df['job_title'] = df['job_title'].apply(lambda x: self.scrub.find_job_titles(str(x)))
        df['job_description'] = df['job_desc'].apply(lambda x: self.scrub.scrub_descriptions(str(x)))

        return df[['job_id','job_timestamp','job_title','job_description']]


    def reduce_to_smallest(self,data):
        """we need to slice the data up by the smallest size """
        data['job_title'] = data['job_title'].fillna('N/A')
        sizes = data.groupby(['job_title']).size()
        smallest_size = sizes.min()
        frame_list = []
        for key in data['job_title'].unique():
            df = data.groupby(['job_title']).get_group(key)
            # shuffle the rows to give them a fair shake and re-index
            df2 = df.sample(frac=1).reset_index(drop=True)
            df3 = df2.truncate(after=smallest_size)
            frame_list.append(df3)
        training = pd.concat(frame_list)
        return training


    def produce_training_data(self):
        df1 = self.scrub_training_data()
        with multiprocessing.Pool(4) as pool:
            results = pool.map(self.read_raw, self.html_list)
            df2 = pd.DataFrame(results, columns=['job_id', 'job_timestamp', 'job_title','job_description'])
            all_data = df1.append(df2)
            all_data.to_csv(self.config()["files"]["full_training"])
            reduced = self.reduce_to_smallest(all_data)
            df_final = reduced.rename(columns={"job_title":"labels","job_description":"data"})
            df_final.to_csv(self.output_file,columns = ['labels','data'])
        # TODO : remove anything with no job description
        return

