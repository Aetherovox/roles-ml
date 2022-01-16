import pandas as pd
import os
from datetime import datetime


def retrieve_file_list(s3_contents):
    file_list = []
    for k in s3_contents:
        if 'raw/reed' in k['Key']:
            file_list.append(k['Key'])
    return file_list


def update_summary(rf, summary_table, keywords, date):
    for kw in keywords:

        if rf.contain_keyword(kw):

            temp_df = pd.DataFrame({'Keywords': [kw], 'Date': [date], 'Count': [1]}, )

            # date and keyword exist already add one to count

            if summary_table['Keywords'][(summary_table['Date'] == date) & (summary_table['Keywords'] == kw)].any():

                summary_table['Count'] = summary_table.apply(lambda row:
                                                             row['Count'] + 1 if row.Keywords == kw
                                                                                 and row.Date == date else row['Count'],
                                                             axis=1)
            else:
                summary_table = pd.concat([summary_table, temp_df])

    return summary_table


def export_file(data, source_loc, filename, s3processor, s3_contents, reed_files):
    """

    :param data: DataFrame to export
    :param source_loc: Location to export to
    :param filename: filename to export as
    :param s3processor: the S3Proccessor object, s3
    :param s3_contents: a list of filenames within S3Proccessor
    :param reed_files: list of reed_files summarised
    :return:
    """
    filename_ext = filename + '.csv'
    archive_filename_ext = filename + '_' + datetime.now().strftime('%Y%m%d%H%M%S') + '.csv'

    file_exists = False
    for k in s3_contents:
        if 'target/' + filename_ext in k['Key']:
            file_exists = True
            break

    if file_exists:
        s3processor.download_file('target/' + filename_ext, source_loc + 'summary_dl.csv')
        s3processor.resource.Object(s3processor.bucket, 'target/' + archive_filename_ext).copy_from(
            CopySource=s3processor.bucket + '/target/' + filename_ext)
        s3processor.resource.Object(s3processor.bucket, 'target/' + filename_ext).delete()

        prev_run = pd.read_csv(source_loc + 'summary_dl.csv')
        data = pd.concat([data, prev_run])
        os.remove(source_loc + 'summary_dl.csv')

    data.to_csv(source_loc + filename_ext, index=False)
    upload_target = s3processor.upload_file(source_loc + filename_ext, filename_ext, "target")
    os.remove(source_loc + filename_ext)

    if upload_target:
        # Delete reed_files
        for files in reed_files:
            s3processor.resource.Object(s3processor.bucket, files).delete()

        # archive dated file
        s3processor.resource.Object(s3processor.bucket, 'raw/archive/' + archive_filename_ext).copy_from(
            CopySource=s3processor.bucket + '/target/' + archive_filename_ext)
        s3processor.resource.Object(s3processor.bucket, 'target/' + archive_filename_ext).delete()

    else:
        # upload to staging
        upload_staging = s3processor.upload_file(source_loc + filename_ext, filename_ext, "staging")
        if upload_staging:
            # if successfully uploaded, move to target
            s3processor.resource.Object(s3processor.bucket, 'target/' + filename_ext).copy_from(
                CopySource=s3processor.bucket + '/staging/' + filename_ext)
            s3processor.resource.Object(s3processor.bucket, 'staging/' + filename_ext).delete()

            # Delete reed files
            for files in reed_files:
                s3processor.resource.Object(s3processor.bucket, files).delete()

            # archive dated file
            s3processor.resource.Object(s3processor.bucket, 'raw/archive/' + archive_filename_ext).copy_from(
                CopySource=s3processor.bucket + '/target/' + archive_filename_ext)
            s3processor.resource.Object(s3processor.bucket, 'target/' + archive_filename_ext).delete()
