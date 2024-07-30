import subprocess

commands = [
    "cd docker-airflow-spark/airflow && docker compose up airflow-init",
    "cd docker-airflow-spark/airflow && docker compose up -d"
]

# docker compose up -d --scale worker=2

for command in commands:
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    if out:
        print(out.decode())
    if err:
        print(err.decode())
