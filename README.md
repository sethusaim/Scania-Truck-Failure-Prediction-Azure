# Scania Truck Failure Prediction Azure System

This is an end to end machine learning system for predicting the failure of the truck engine component Air Pressure System. This entire solution is built using Azure Services like Azure Blob Storage (for storing the data), Azure CosmoDB MongoDB API (for logging and improvising the system performance and for database operations), Azure Container Registry (for storing the container images), and Azure  Container Instances (for running the container image). Apart from Azure services, MLFlow was used for experiment tracking and model versioning and model staging with artifacts stored in Azure Blob Storage. Docker for containerization of application. 

### Problem Statement 
To build a classification methodology to predict the failure of Air Pressure System on the basis of given set of predictors. 

### Approach to building the solution
In the first place, whenever we start a machine learning project, we need to sign a data sharing agreement with the client, where sign off some of the parameters like,

    1. Format of data - like csv format or json format,etc
    2. Number of Columns
    3. Length of date stamp in the file
    4. Length of time stamp in the file
    5. DataType of each sensor - like float,int,string

Once the data sharing agreement,is created we create a master data management, which is nothing but the schema_training.json and schema_prediction.json file. Using this data we shall validate the batch data which is sent to us. 

The data which is sent to us will be stored in Azure Blob Storages. From Azure Blob Storages, using schema file, the data is validated againist filename, column length, and missing values in the column. 

Once the data validation is done, the data transformation pipeline is triggered, where we add quotes to string values in the data.After the data transformation is done, The good data is stored is stored in MongoDB and once is stored in database, we will export a csv file which will be used training for the models.

The model training is done by using a customized machine learning approach,in which the entire training data is divided to clusters using KMeans algorithm, and for every cluster of data, a model is trained and then model is used for prediction. So before we apply a clustering algorithm to the data, we need to preprocess the data as done in the jupyter notebook like  missing values, replacing invalid values. Then elbow plot is created and number of clusters is created and based on the number of clusters XGBoost model and Random Forest Model are trained and are saved in Azure Blob Storage.

Once the models are trained,they are tested againist the test data and model score is found out.Now MLFlow is used for logging the parameters,metrics and models to the server. Once the logging of parameters,metrics and models is done. A load production model is triggered to which will get the top models based on metrics and then transitioned to production or staging depending on the condition.

### Post Model Training
The solution application is exposed as API using FastAPI and application is dockerized using Docker. MLFlow setup is done in an Azure Virtual Machine. CI-CD pipeline is created which will deploy the application to Azure Container Instances, whenever new code is commmited to GitHub.

#### Technologies Used 
- Python
- Sklearn for machine learning algorithms
- FastAPI for creating an web application
- Machine Learning
- MLFlow for experiment tracking,model versioning and model staging.
- SQL-Lite as backend store for MLFlow server
- Azure Virtual Machine for deploying the MLFlow server
- Azure Azure Blob Storages for data storage
- Azure CosmoDB MongoDB API for storing the logging data and database operations
- Azure Container Registry for storing the container images
- Azure Container Instances for running the container applications

### Algorithms Used 
- Random Forest Classifier Model
- XGBoost Classifier Model

### Metrics 
- Accuracy
- ROC AUC score

### Cloud Deployment 
- CI-CD deployment to Azure Container Instances
