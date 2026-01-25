## For development

Open the project in VSCode and >Devcontainer: Open Folder in Container or something similar. 
Run alembic upgrade head in terminal

# For evaluation
Start the containers with the following command and attach to the main container with a terminal:

docker compose -f .devcontainer/docker-compose.prod.yml up -d && docker attach dia_eval

Once the terminal is ready, run

For task 1
python task_1_etl_pipeline.py 

For task 2
python task_2_sql_queries.py

For task 3


