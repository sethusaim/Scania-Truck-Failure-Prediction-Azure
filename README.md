
# Scania Truck Failure Prediction

This is an end to end machine learning system with MLFlow integration for predicting the quality of wafer sensors.

## Demo
- AWS Link - http://3.216.31.92:8080/ (not avaiable)

## Table of Contents
- Problem Statement
- How to run the application
- Technologies used 
- Proposed Solution and Architecture
- WorkFlow of project
- Technologies used 

### Problem Statement 
Improper maintenance on a machine or system impacts to worsen mean time between failure (MTBF). Manual diagnostic procedures tend to extended downtime at the system breakdown. Machine learning techniques based on the internet of things (IoT) sensor data were used to make predictive maintenance to determine whether the sensor needs to be replaced or not. 

## How to implement the project

- Create a conda environment 

```bash
conda create -n scania-ops python=3.6.9 -y
```

- Activate the environment
```bash
conda activate scania-ops
```

- Install the requirements.txt file
```bash
pip install -r requirements.txt
```

To run the application, execute these commands.

```bash 
uvicorn app:app --reload
```
Build the docker image

```bash
docker build . -t imagename
```

### WorkFlow of the Project 
To solve the problem statement we have proposed a customized machine learning approach. 

![WorkFlow of Project](https://github.com/sethusaim/Wafer-Fault-Prediction-using-MlFlow/blob/main/docs/Other/Wafer%20Architecture.jpg?raw=True)

In the first place, whenever we start a machine learning project, we need to sign a data sharing agreement with the client, where sign off some of the parameters like,

- Format of data - like csv format or json format,etc
- Number of Columns 
- Length of date stamp in the file 
- Length of time stamp in the file
- DataType of each sensor - like float,int,string

The client will send multiple set of files in batches at a given location. In our case, the data which will be given to us, will consist of encrypted columns names and 60000 rows and 171 columns of different values for each components sensor data.

The last column will have class value for each feature as per the data sharing agreement

- neg indicates negative class
- pos indicates positive class

These data can be found in the schema training json file.More details are present in LLD documentation of project.

### Technical Aspects of the Project

As discussed, the client will send multiple set of files in batches at a given location. After signing the data sharing agreement, we create the master data management which is nothing but the schema training json file and schema Prediction json (this is be used for Prediction data).
We have divided the project into multiple modules, for high level understanding some of them are 

#### Training Validation
In this module,we will trigger the training validation pipeline,which will be responsible for training validation. In the training validation pipeline,we are internally triggering some of the pipelines,
some of the internal function are 
 - Training raw data validation - This function is responsible for validating the raw data based on schema training json file, and we have manually created a regex pattern for validating the filename of the data. We are even validating length of date time stamp, length of time stamp of the data. If some of the data does not match the criteria of the master data management, if move that files to bad folder and will not be used for training or Prediction purposes.

 - Data Transformation - Previously, we have created both good and bad directory for storing the data based on the master data management. Now for the data transformation we are only performing the data transformation on good data folder. In the data transformation, we replace the missing values with the nan values.

 - DataBase Operation - Now that we have validated the data and transformed the data which is suitable for the further training purposes. In database operation we are using SQL-Lite. From the good folder we are inserting the data into a database. After the insertion of the data is done we are deleting the good data folder and move the bad folder to archived folder. Next inserting the good database, we are extracting the data from the database and converting into csv format.


#### Training Model
In the previous pipeline,after the database operation, we have exported the good data from database to csv format. In the training model pipeline, we are first fetching the data from the exported csv file.

Next comes the preprocessing of the data, where we are performing some of the preprocessing functions such as remove columns, separate label feature, imputing the missing the values if present. Dropping the columns with zero standard deviation.

As mentioned we are trying to solve the problem by using customized machine learning approach.We need to create clusters of data which represents the variation of data. Clustering of the data is based on K-Means clustering algorithm.

For every cluster which has been created two machine learning models are being trained which are RandomForest and XGBoost models with GridSearchCV as the hyperparameter tuning technique. The metrics which are monitoring are accuracy and roc auc score as the metric.

After training all the models, we are saving them to trained models folders. 

Now that the models are saved into the trained models folder, here the mlops part comes into picture, where in for every cluster we are logging the parameters, metrics and models to mlflow server. On successful completion of training of all the models and logging them to mlflow, next pipeline will be triggered which is load production model pipeline.

Since all the trained models, will have different metrics and parameters, which can productionize them based on metrics. 
For this project we have trained 6 models and we will productionize 3 models along with KMeans model for the Prediction service.

Here is glimpse of the mlflow server showing stages of the models (Staging or Production based on metrics)

![mlflow server image](https://github.com/sethusaim/Wafer-Fault-Prediction-using-MlFlow/blob/main/docs/Other/MLOPS%20server%20page.png?raw=True)


### Prediction pipeline
The Prediction pipeline will be triggered following Prediction validation and Prediction from the model. In this Prediction pipeline, the same validation steps like validating file name and so on. The Prediction pipeline, and the preprocessing of Prediction data. For the Prediction, we will load the trained kmeans model and then predict the number of clusters, and for every cluster, model will be loaded and the Prediction will be done. The Predictions will saved to Predictions.csv file and then Prediction is completed.


#### Technologies Used 
- Python
- Sklearn
- FastAPI 
- Machine Learning
- Numpy
- Pandas 
- MlFlow
- MongoDB
- ECR and ECS
- S3 buckets

### Algorithms Used 
- Random Forest 
- XGBoost 

### Metrics 
- Accuracy
- ROC AUC score

### Cloud Deployment 
- AWS EC2 instance for MLFlow server
- AWS ECS cluster for Application.
