FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN apt update
RUN apt upgrade -y
RUN pip install -r requirements.txt

CMD ["uvicorn" ,"app.main:app" , "--host", "0.0.0.0"]
