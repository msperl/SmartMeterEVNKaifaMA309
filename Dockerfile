FROM python:slim-bullseye

WORKDIR /app

COPY requirements.txt /app

RUN pip3 install -r /app/requirements.txt

COPY EvnSmartmeterMQTTKaifaMA309.py home_assistant_mqtt.py /app/

ENV SERIAL_PORT=/dev/serial

CMD ["python3", "/app/EvnSmartmeterMQTTKaifaMA309.py"]
