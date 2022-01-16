from src.S3Processor import S3Processor
from src.ReedFile import ReedFile
import src.KeywordProcessor as kp
import pandas as pd
import json
import os


if __name__ == '__main__':

    # Define keywords from json file
    with open("config/keywords.json", "r") as read_file:
        keywords_dict = json.load(read_file)

    keywords = keywords_dict['skills']

    s3 = S3Processor()
    s3_contents = s3.client.list_objects(Bucket=s3.bucket)['Contents']

    file_list = kp.retrieve_file_list(s3_contents)

    summary = pd.DataFrame(columns=['Keywords', 'Date', 'Count'])

    dl_location = ".\\data"

    for file in file_list:

        filename = file[4:]
        s3.download_file(file, dl_location + filename)
        rf = ReedFile(dl_location, filename)

        summary = kp.update_summary(rf, summary, keywords, rf.date)
        os.remove(dl_location + filename)

    kp.export_file(summary, dl_location, "summary", s3, s3_contents, file_list)



