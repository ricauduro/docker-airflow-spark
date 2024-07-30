import subprocess

def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process.communicate()

def run(command):
    out, err = run_command(command)
    if out:
        return print(out.decode())
    if err:
        return print(err.decode())

imagens = ["python", "jupyter", "airflow", "mysql"]
imagens_existentes = []

# Verificar se as imagens necessarias existem e criar se necessario
out, err = run_command("docker images")
if out:
    output_lines = out.decode().split("\n")
    for n, line in enumerate(output_lines):
        if n > 0 and len(line) > 0:
            imagens_existentes.append(line.split("-")[1].split(" ")[0])
            
if err:
    print(err.decode())

criar = [imagem for imagem in imagens if imagem not in imagens_existentes]

# Criar as imagens necessarias
if len(criar) > 0:
    _ = [run(f"docker build --tag rica-{imagem}:v1.0 .") for imagem in criar]


# Iniciar o Airflow / Docker
commandos = [
    "cd docker-airflow-spark/airflow && docker compose up airflow-init",
    "cd docker-airflow-spark/airflow && docker compose up -d"
]

_ = [run(commando) for commando in commandos]

