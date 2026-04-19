FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas pymongo psutil

COPY 02_hil_simulation.py .
COPY simulation_inputs.csv .

CMD ["python", "-u", "02_hil_simulation.py"]
