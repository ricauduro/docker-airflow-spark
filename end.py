import subprocess

def run_command(command):
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return process.communicate()

# Parar o Docker e remover volumes e images
command = "cd docker-airflow-spark/airflow && docker compose down --volumes --rmi all"
out, err = run_command(command)
if out:
    print(out.decode())
if err:
    print(err.decode())

# Verificar imagens com a tag <none> e apagar
out, err = run_command("docker images")
if out:
    output_lines = out.decode().split("\n")
    for line in output_lines:
        if "none" in line:
            image_id = line.split('<none>')[2].split('    ')[1].split(' ')[0]
            out, err = run_command(f"docker rmi {image_id}")
            if out:
                print(out.decode())
            if err:
                print(err.decode())
if err:
    print(err.decode())
