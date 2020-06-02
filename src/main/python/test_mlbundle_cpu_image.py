import argparse
import logging
import os
import random
import subprocess
import sys
import time
import warnings
from datetime import datetime
from pyspark.sql import SparkSession

# Basic import test for Dask and PyArrow

import dask
from dask.distributed import Client, LocalCluster
import dask.array as da
import dask.dataframe as dd
import pyarrow
import pyarrow.parquet
import pyarrow.csv

classpath = os.environ.get('CLASSPATH', "")
hadoop_prefix = os.environ.get('HADOOP_PREFIX', "")

hadoop_path = os.path.join(hadoop_prefix, 'bin', 'hadoop')
if hadoop_path is not "" and hadoop_prefix is not "":
    print("hadoop_path", hadoop_path)
    hadoop_classpath = subprocess.check_output(
        [hadoop_path, 'classpath', '--glob']).decode()
    logging.debug("CLASSPATH: {0}".format(classpath))
    logging.debug("HADOOP_CLASSPATH: {0}".format(hadoop_classpath))
    os.environ['CLASSPATH'] = classpath + os.pathsep + hadoop_classpath


def main():
    from absl import app as absl_app
    from absl import flags
    print("Initializing spark")

    spark = SparkSession\
        .builder\
        .appName("test_mlbundle_cpu_image.py")\
        .getOrCreate()

    import tensorflow as tf

    print("{0} ===== Start".format(datetime.now().isoformat()))

    num_gpus = 0
    print("args:")
    for i in range(len(sys.argv)):
        print("    {}".format(sys.argv[i]))
        if sys.argv[i] == "--num_gpus":
            num_gpus = int(sys.argv[i + 1])

    print("Loading primary machine learning frameworks")
    import tensorflow as tf
    print("Tensorflow version: {}".format(tf.__version__))

    import xgboost as xgb
    print("XGBoost version: {}".format(xgb.__version__))

    import lightgbm as lgb
    print("LightGBM version: {}".format(lgb.__version__))

    import catboost
    print("catboost version: {}".format(catboost.__version__))

    import torch
    print("PyTorch version: {}".format(torch.__version__))

    print("Testing ResNet50 with Tensorflow -- GPUs are in use if defined.")
    from tensorflow.keras.applications.resnet50 import ResNet50
    import time
    import numpy as np
    model = ResNet50(weights=None)
    data = np.random.rand(123, 224, 224, 3)
    ms = time.time()
    model.predict(data, batch_size=32)
    print("Prediction duration".format(time.time() - ms))

    print("Testing XGBoost")
    from gzip import GzipFile
    from sklearn.datasets import load_breast_cancer
    from sklearn.model_selection import train_test_split

    dataset = load_breast_cancer()
    X = dataset.data
    y = dataset.target

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.25, train_size=0.75, random_state=5566)

    num_round = 100

    param = {
        'objective': 'binary:logistic',
        'max_depth': 5,
        'eta': 1
    }

    dtrain = xgb.DMatrix(X_train, label=y_train)
    dtest = xgb.DMatrix(X_test, label=y_test)
    gpu_res = {}
    tmp = time.time()
    bst = xgb.train(
        param, dtrain, num_round, evals=[
            (dtest, 'test')], evals_result=gpu_res)
    print("CPU Training Time: %s seconds" % (str(time.time() - tmp)))

    print("Writing/Restoring XGB model to HDFS")
    import subprocess
    subprocess.check_output(["hadoop", "fs", "-rm", "-f", "/tmp/deleteme.xgb"])

    bst.save_model("hdfs:///tmp/deleteme.xgb")
    model = xgb.Booster()
    model.load_model("hdfs:///tmp/deleteme.xgb")

    subprocess.check_output(["hadoop", "fs", "-rm", "-f", "/tmp/deleteme.xgb"])
    print("Print out first 10 predictions:")
    print(model.predict(dtest)[:10])

if __name__ == '__main__':
    main()
