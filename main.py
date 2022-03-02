import json
import os

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from fastapi.templating import Jinja2Templates

from scania.model.load_production_model import Load_Prod_Model
from scania.model.prediction_from_model import Prediction
from scania.model.training_model import Train_Model
from scania.validation_insertion.prediction_validation_insertion import Pred_Validation
from scania.validation_insertion.train_validation_insertion import Train_Validation
from utils.create_containers import Azure_Container
from utils.read_params import read_params

os.putenv("LANG", "en_US.UTF-8")
os.putenv("LC_ALL", "en_US.UTF-8")

app = FastAPI()

config = read_params()

templates = Jinja2Templates(directory=config["templates"]["dir"])

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        config["templates"]["index_html_file"], {"request": request}
    )


@app.get("/create")
async def create_containers():
    try:
        container = Azure_Container()

        container.generate_containers()

        return Response(f"Error Occurred!{e}")

    except Exception as e:
        return Response(f"Error Occurred!{e}")


@app.get("/train")
async def trainRouteClient():
    try:
        raw_data_train_container_name = config["container"]["scania_raw_data"]

        train_val_obj = Train_Validation(container_name=raw_data_train_container_name)

        train_val_obj.training_validation()

        train_model_obj = Train_Model()

        num_clusters = train_model_obj.training_model()

        Load_Prod_Model_obj = Load_Prod_Model(num_clusters=num_clusters)

        Load_Prod_Model_obj.load_production_model()

    except Exception as e:
        return Response(f"Error Occurred!{e}")

    return Response("Training successfull!!")


@app.get("/predict")
async def predictRouteClient():
    try:
        raw_data_pred_container_name = config["container"]["scania_raw_data_container"]

        pred_val = Pred_Validation(raw_data_pred_container_name)

        pred_val.prediction_validation()

        pred = Prediction()

        container, filename, json_Predictions = pred.predict_from_model()

        return Response(
            f"Prediction file created in {container} container with filename as {filename}, and few of the Predictions are {str(json.loads(json_Predictions))}"
        )

    except Exception as e:
        return Response(f"Error Occurred! {e}")


if __name__ == "__main__":
    host = config["app"]["host"]

    port = config["app"]["port"]

    uvicorn.run(app, host=host, port=port)
