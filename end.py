import subprocess

commands = [
    "cd docker-airflow-spark/airflow && docker compose down --volumes --rmi all"
]

for command in commands:
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = process.communicate()
    if out:
        print(out.decode())
    if err:
        print(err.decode())
