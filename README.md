# api-francaises-des-dev
           
## To set up the project

Install dependence :

```
pip install -r requirements.txt 
```

Change .env.example to set up the database :

```
MYSQL_USER = ""
MYSQL_PASSWORD = ""
MYSQL_DATABASE = ""
MYSQL_PORT = 3306
MYSQL_HOST = "localhost"
```

remove .example extension from the file .env.example :

    ```
    mv .env.example .env
            OR
    cp .env.example .env
    ```

To start the server :

```
uvicorn app.main:app --reload
```

>⚠️ You need a virtual environment -> see the FastAPI document
 To create a virtual environment :

```
python -m venv venv
```

To activate the virtual environment :

```
source venv/bin/activate
```

To deactivate the virtual environment :

```
deactivate
```
