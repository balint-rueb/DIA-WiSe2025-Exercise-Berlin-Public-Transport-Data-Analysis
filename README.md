## For development

Open the project in VSCode and >Devcontainer: Open Folder in Container or something similar. 
Run alembic upgrade head in terminal

# For evaluation
ENSURE TIMETABLES AND TIMETABLE_CHANGES FOLDERS ARE LOCATED IN THE ROOT OF THIS PROJECT. OTHERWISE IT WONT WORK!
THE DATA INSIDE SHOULD REMAIN IN TAR.GZ. DO NOT EXTRACT!

1.  Pull code from here. The project contains the train data, meaning it is completely self contained. 
2. Start the containers with the following command:
    docker compose -f .devcontainer/docker-compose.prod.yml up -d --build && docker attach dia_eval

3. Once the terminal is ready, run
For task 1
python task_1_etl_pipeline.py 

For task 2
python task_2_sql_queries.py

For task 3
python task_3_1.py
python task_3_2.py
python task_3_3.py

For task 4
python task_4_1.py


