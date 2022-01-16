from src.S3Processor import S3Processor
from src.models import Classifier
from config.files import files
from src.data import JobData
import pandas as pd


def bucket_pull():
    # todo: edit S3 processor so that if the file exists in the target directory we continue to the next iteration
    s3 = S3Processor()
    # s3.upload_file(f"./files/{files['ml_file']}",files['ml_file'])
    s3.download_file(files['ml_file'])
    return

def compile_training_data():
    s3 = S3Processor()
    s3.download_all()
    x = JobData()
    x.produce_training_data()
    return

def generate_model_debug_samples():
    return open('./files/jobsamples.txt').readlines()

def main():

    # instantiate our classifier model (trained on instantiation)
    classy = Classifier()
    print("Dimensions of training data: {} ".format(classy.x_termcounts.shape))

    # build analysis data from raw

    model_input = generate_model_debug_samples()
    predictions = classy.predict(model_input)
    df = pd.DataFrame(list(zip(predictions,model_input)), columns = ["Prediction","Job Description"])
    df.to_csv(files['outfile'])
    print(df.head())


if __name__ == '__main__':
    bucket_pull()
    main()
