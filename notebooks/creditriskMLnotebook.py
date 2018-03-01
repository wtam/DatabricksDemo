# Databricks notebook source
# MAGIC %sh git init

# COMMAND ----------

# init script for the cluster, https://docs.databricks.com/user-guide/clusters/init-scripts.html 
dbutils.fs.mkdirs("dbfs:/databricks/init/")

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/databricks/init/"))

# COMMAND ----------

# enable inline plotting with matplotbib and seaborn
%matplotlib inline

import numpy as np
import pylab as pl
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns; sns.set(style="ticks", color_codes=True)
import pydotplus 
from IPython.display import Image  
from collections import defaultdict
import sklearn
from sklearn.dummy import DummyClassifier
from sklearn import svm, tree
from sklearn.tree import export_graphviz
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV
from sklearn.preprocessing import OneHotEncoder, LabelEncoder, label_binarize, StandardScaler, MinMaxScaler
from sklearn.decomposition import PCA
from sklearn.metrics import classification_report,confusion_matrix, roc_curve, auc
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.ensemble import ExtraTreesClassifier, RandomForestClassifier, GradientBoostingClassifier, AdaBoostClassifier, BaggingClassifier, VotingClassifier
from sklearn.feature_selection import RFE, SelectKBest, chi2, SelectFromModel
from sklearn.linear_model import LogisticRegression, SGDClassifier
#from imblearn.over_sampling import SMOTE
#--Note: you can import the pkg thru the PyPI(Python Package Index) under the Azure Databricks's Library

# COMMAND ----------

# Function for evaluation reports
def get_eval(clf, X_train, y_train,y_test,y_pred):
    # Cross Validation to test and anticipate overfitting problem
    scores1 = cross_val_score(clf, X_train, y_train, cv=10, scoring='accuracy')
    scores2 = cross_val_score(clf, X_train, y_train, cv=10, scoring='precision')
    scores3 = cross_val_score(clf, X_train, y_train, cv=10, scoring='roc_auc')
    # The mean score and standard deviation of the score estimate
    print("Cross Validation Accuracy: %0.2f (+/- %0.2f)" % (scores1.mean(), scores1.std()))
    print("Cross Validation Precision: %0.2f (+/- %0.2f)" % (scores2.mean(), scores2.std()))
    print("Cross Validation roc_auc: %0.2f (+/- %0.2f)" % (scores3.mean(), scores3.std()))
    # Create and print confusion matrix
    abclf_cm = confusion_matrix(y_test,y_pred)
    print(abclf_cm)
    return

# Function to get roc curve
def get_roc (y_test,y_pred):
    # Compute ROC curve and ROC area for each class
    fpr = dict()
    tpr = dict()
    roc_auc = dict()
    fpr, tpr, _ = roc_curve(y_test, y_pred)
    roc_auc = auc(fpr, tpr)
    #Plot of a ROC curve
    plt.figure()
    lw = 2
    plt.plot(fpr, tpr, color='darkorange',
             label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], color='navy', lw=lw, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.0])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    plt.show()
    return

# COMMAND ----------

#Random Forest Classifier
# fit, train and cross validate Decision Tree with training and test data 
def randomforestclf(X_train, y_train,X_test, y_test):
    print("RandomForestClassifier")
    randomforest = RandomForestClassifier().fit(X_train, y_train)

    # Predict target variables y for test data
    y_pred = randomforest.predict(X_test)

    # Get Cross Validation and Confusion matrix
    get_eval(randomforest, X_train, y_train,y_test,y_pred)
    return

# COMMAND ----------

# SGDClassifier
# fit, train and cross validate Decision Tree with training and test data 
def sgdclf(X_train, y_train,X_test, y_test):
    print("SGDClassifier")
    sgd = SGDClassifier().fit(X_train, y_train)

    # Predict target variables y for test data
    y_pred = sgd.predict(X_test)

    # Get Cross Validation and Confusion matrix
    get_eval(sgd, X_train, y_train,y_test,y_pred)
    return

# COMMAND ----------

# Neural Network: MLPClassifier
# fit, train and cross validate KNeighborsClassifier with training and test data 
# good working example with tensorflow https://github.com/andpol5/credit-classifier/blob/master/trainNeuralNet.py
# Choose hidden layer size: https://stats.stackexchange.com/questions/181/how-to-choose-the-number-of-hidden-layers-and-nodes-in-a-feedforward-neural-netw
def mlpclf(X_train, y_train, X_test, y_test): 
    print("MLPClassifier")
    # fit MLPClassifier with training data, good hidden_layer_sizes = 5,5:AUC63;75,75:AUC65;
    mlpclf = MLPClassifier(solver='lbfgs', alpha=5, hidden_layer_sizes=(75,75))
    mlpclf.fit(X_train, y_train)
    
    print(mlpclf.get_params)
    
    # Predict target variables y for test data
    y_pred = mlpclf.predict(X_test)
    
    # Get Cross Validation and Confusion matrix
    get_eval(mlpclf, X_train, y_train,y_test,y_pred)
    return

# COMMAND ----------



# COMMAND ----------

#--Mounting Azure Data Lake Stores with DBFS (Databricks File System)
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "55f2dd64-39d7-4fa5-a12b-5da3bf6e1d50",
           "dfs.adls.oauth2.credential": "yeZZZBYVlWvdbAhPA4EulPtNrlai0GZwdb6vOoHwl8Q=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/4dced229-4c95-476d-b76b-34d306d723eb/oauth2/token"}

dbutils.fs.mount(source = "adl://storebotdatalakestore.azuredatalakestore.net/clusters/sparkcluster", 
                 mount_point = "/mnt/clusters/sparkcluster",
                 extra_configs = configs)

# COMMAND ----------

# Import original categorical/numerical data
# import categorical/numerical data (.csv) with Pandas DataFrame
# adl://storebotdatalakestore.azuredatalakestore.net/clusters/sparkcluster/GermanCreditUCIdataset.csv
filename = '/dbfs/mnt/clusters/sparkcluster/GermanCreditUCIdataset.csv'
names = ['existingchecking', 'duration', 'credithistory', 'purpose', 'creditamount', 
         'savings', 'employmentsince', 'installmentrate', 'statussex', 'otherdebtors', 
         'residencesince', 'property', 'age', 'otherinstallmentplans', 'housing', 
         'existingcredits', 'job', 'peopleliable', 'telephone', 'foreignworker', 'classification']
data = pd.read_csv(filename, skiprows=1, names = names)
print(data.shape)
print (data.columns)
data.head(10)

# COMMAND ----------

#create quickaccess list with categorical variables labels
catvars = ['existingchecking', 'credithistory', 'purpose', 'savings', 'employmentsince',
           'statussex', 'otherdebtors', 'property', 'otherinstallmentplans', 'housing', 'job', 
           'telephone', 'foreignworker']
#create quickaccess list with numerical variables labels
numvars = ['creditamount', 'duration', 'installmentrate', 'residencesince', 'age', 
           'existingcredits', 'peopleliable', 'classification']

# COMMAND ----------

# Data Cleaning : Missing Values
print(data.isnull().sum().sum())
# No missing values

# COMMAND ----------

# No outliers
# Binarize the y output for easier use of e.g. ROC curves -> 0 = 'bad' credit; 1 = 'good' credit
# original data is 1 is good and 2 is bad credit
data.classification.replace([1,2], [1,0], inplace=True)
# Print number of 'good' credits (should be 700) and 'bad credits (should be 300)
data.classification.value_counts()

# COMMAND ----------

# Boxplots for continuous attributes
fig, axes = plt.subplots(nrows=1, ncols=3)
axes[0].boxplot(data['creditamount'])
axes[0].set_title('creditamount')
axes[1].boxplot(data['duration'])
axes[1].set_title('duration')
axes[2].boxplot(data['age'])
axes[2].set_title('age')

fig.savefig("boxplots.png")

# COMMAND ----------

# Numerical features range of values
for x in range(len(numvars)):
    print(numvars[x],": ", data[numvars[x]].min()," - ",data[numvars[x]].max())

# COMMAND ----------

# Standardization
numdata_std = pd.DataFrame(StandardScaler().fit_transform(data[numvars].drop(['classification'], axis=1)))
# MinMax Rescaling to [0,1]
numdata_minmax = pd.DataFrame(MinMaxScaler().fit_transform(data[numvars].drop(['classification'], axis=1)))
print("success")

# COMMAND ----------

# Encoding categorical features
#
# Labelencoding to transform categorical to numerical
# Enables better Visualization than one hot encoding
d = defaultdict(LabelEncoder)

# Encoding the variable
lecatdata = data[catvars].apply(lambda x: d[x.name].fit_transform(x))

# print transformations
for x in range(len(catvars)):
    print(catvars[x],": ", data[catvars[x]].unique())
    print(catvars[x],": ", lecatdata[catvars[x]].unique())

# COMMAND ----------

# One hot encoding
#create dummy variables for every category of every categorical variable
dummyvars = pd.get_dummies(data[catvars])

# COMMAND ----------

# append the dummy variable of the initial numerical variables numvars
data_clean = pd.concat([data[numvars], dummyvars], axis = 1)
numdata_std.reset_index(drop=True)

data_std = pd.concat([numdata_std, data['classification'], dummyvars], axis = 1)
data_minmax = pd.concat([numdata_minmax, data['classification'], dummyvars], axis = 1)
print(data_clean.shape)
print(data_std.shape)
print(data_minmax.shape)

# COMMAND ----------

# Split Training and Test Data
#

X_train_clean, X_test_clean, y_train_clean, y_test_clean = train_test_split(X_clean,y_clean,test_size=0.3333, random_state=1)


# COMMAND ----------

# Random Forest
# Choose clean data, as tree is robust
randomforestclf(X_train_clean, y_train_clean,X_test_clean, y_test_clean)

# COMMAND ----------

# Multi-layer Perceptron classifier (MLP)
mlpclf(X_train_clean, y_train_clean, X_test_clean, y_test_clean)