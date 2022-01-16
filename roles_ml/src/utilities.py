import csv
from src.decorators import timer

# todo: define timer decorator class

def produce_output(outfile,model_input,predictions):
    with open(outfile ,'w', newline='',encoding='utf-8') as file:
        writer = csv.writer(file,delimiter = '|' , quotechar='"',quoting = csv.QUOTE_MINIMAL)
        writer.writerow(["jobid","input","prediction"])
        for pred in zip(list(range(len(model_input))), model_input, predictions):
            i , j ,k = pred
            writer.writerow([i,j,k])

def build_sample_data():
    samples = "./files/jobsamples.txt"
    sample_data = []
    with open(samples) as r:
        line = r.read()
        # print(line)
        sample_data.append(line)
    return sample_data


# todo: currently inefficient, change
@timer
def unpacker(packed):
    training_data = {'data':[],'labels':[]}
    minmax = min([len(packed[k]) for k in packed])
    print(minmax)
    for k in packed:
        print("Job Type: {} has {} descriptions.".format(k,len(packed[k])))
        for i,values in enumerate(packed[k]):
            # todo: replace this with max i lookup
            if i > minmax:
                break
            training_data['data'].append(values)
            training_data['labels'].append(k)
    return training_data



