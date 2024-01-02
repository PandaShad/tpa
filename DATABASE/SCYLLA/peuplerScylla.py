import subprocess

def run_script(script_name):
    print(f"----- Début de l'exécution de {script_name} -----")
    subprocess.run(["python3", script_name], check=True)
    print(f"----- Fin de l'exécution de {script_name} -----")

def main():
    try:
        run_script("peuplerScyllaClients.py")
        run_script("peuplerScyllaMarketing.py")
        run_script("peuplerScyllaCatalogue.py")
        run_script("peuplerScyllaImmatriculations.py")
        print("Tous les scripts ont été exécutés avec succès.")
    except subprocess.CalledProcessError as e:
        print(f"Une erreur s'est produite lors de l'exécution du script: {e}")

if __name__ == "__main__":
    main()

