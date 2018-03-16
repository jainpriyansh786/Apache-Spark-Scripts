import os

import zipfile

from  urllib import request


def download_data() :

    dataset_url = 'http://files.grouplens.org/datasets/movielens/ml-latest.zip'

    download_path = os.path.join('./','data')

    data_file = request.urlretrieve(dataset_url,download_path)

    with zipfile.ZipFile(download_path+'/ml-latest.zip', "r") as z:
       z.extractall(download_path)


if __name__ == "__main__":
    download_data()






