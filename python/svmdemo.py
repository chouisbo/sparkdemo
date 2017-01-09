from pyspark import SparkConf, SparkContext
from pyspark.mllib.util import MLUtils
from pyspark.mllib.classification import SVMWithSGD, SVMModel


# SparkMaster = "yarn-client"
# SparkAppName = "PySparkApp SVM demo"
# HDFS_ROOT = "hdfs://nameservice1"
SparkMaster = "spark://host29:7077"
SparkAppName = "PySparkApp wordcount demo"
HDFS_ROOT = "hdfs://ict24:8020"


if __name__ == "__main__":

    conf = SparkConf().setMaster(SparkMaster).setAppName(SparkAppName)
    sc = SparkContext(conf = conf)

    # Load the data
    data = MLUtils.loadLibSVMFile(sc, HDFS_ROOT+"/dataset/heart_scale")
    parsedData = data.map(lambda p: (0.0, p.features) if p.label != 1.0 else p)                                                                              

    # Build the model
    model = SVMWithSGD.train(parsedData, iterations=100)

    # Evaluating the model on training data
    labelsAndPreds = data.map(lambda p: (p.label, model.predict(p.features)))
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(data.count())
    print("Training Error = " + str(trainErr))

    
