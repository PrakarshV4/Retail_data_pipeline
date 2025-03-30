
# Project Setup Guide

This guide will help you set up and run the project using Docker Compose.

## 1. Clone the Repository
Run the following command to clone the repository:
```sh
git clone https://github.com/PrakarshV4/Retail_data_pipeline.git
```
Replace `<repo_url>` with the actual repository URL.

## 2. Navigate into the Repository
```sh
cd Retail_data_pipeline
```
Replace `<repo_name>` with the cloned repository name.

## 3. Start the Services with Docker Compose
Run the following command to build and start the containers:
```sh
docker compose up -d
```
The `-d` flag runs the containers in detached mode.

## 4. Verify Running Containers
Check if all required containers are running:
```sh
docker ps
``` 
## 5. Access Running Services
- **Airflow UI**: [http://localhost:8080](http://localhost:8080)  
- **Jupyter Notebook**: [http://localhost:8888](http://localhost:8888)
  - In jupyter Notebook go to /home/jovyan/work folder and open notebook. 
- **PostgreSQL Database**:  

## 6. Stop and Remove Containers
To stop the running containers:
```sh
docker compose down
```
To remove all containers, volumes, and networks:
```sh
docker compose down -v
```

---

Now you're all set! 🚀 Let me know if you need any modifications.
