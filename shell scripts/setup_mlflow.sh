conda create -n ENV_NAME python=3.6.9 -y

conda activate ENV_NAME

pip3 install mlflow 

pip3 install awscli

echo "Installed mlflow"

sudo apt install nginx 

echo "Installed nginx"

sudo apt-get install apache2-utils 

echo "Installed apache2-utils"

sudo htpasswd -c /etc/nginx/.htpasswd MLFLOW_AUTH_USER_NAME

cd /etc/nginx/sites-enabled

echo "server {
    listen 8080;
    server_name YOUR_PUBLIC_IP;
    auth_basic “Administrator-Area”;
    auth_basic_user_file /etc/nginx/.htpasswd; 

    location / {
        proxy_pass http://localhost:8000;
        include /etc/nginx/proxy_params;
        proxy_redirect off;
    }
}
" >> mlflow

echo "added mlflow nginx configuration file"

sudo service nginx start

echo "started nginx"

cd /home/ubuntu

cd  /etc/systemd/system

echo "
[Unit]
Description=MLflow tracking server
After=network.target

[Service]
Restart=on-failure
RestartSec=30

ExecStart=/bin/bash -c 'PATH=/home/ubuntu/anaconda3/envs/mlflow/bin/:$PATH exec mlflow server --backend-store-uri sqlite:///mlflow.db --default-artifact-root s3://scania-mlflow/ --host 0.0.0.0 -p 8000'

[Install]
WantedBy=multi-user.target
" >> mlflow-tracking.service

echo "added service file for mlflow"

cd /home/ubuntu

sudo systemctl daemon-reload

echo "daemon reloaded"

sudo systemctl enable mlflow-tracking 

echo "enabled mlflow-tracking"

sudo systemctl start mlflow-tracking

echo "started mlflow-tracking as service"

aws configure

echo "configured aws details"

echo "mlflow setup is done"

echo "close the connection, stop and start your instance and then check your ip with port 8080"