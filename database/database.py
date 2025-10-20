import psycopg2
import os


SQL_SCRIPTS = [
    "DimCategory.sql",
    "DimState.sql",
    "DimFootnotes.sql",
    "DimFrequency.sql",
    "FactMinimumWage.sql",
    "BridgeFactMinimumWageFootnote.sql"
]

def config_database(sql_dir="database/sql"):
    """Executa os scripts SQL para configurar o banco."""
    try:
        conn = psycopg2.connect(**DATABASE_CONFIG)
        conn.autocommit = True
        cur = conn.cursor()

        # Garantir que o schema public seja usado
        cur.execute("SET search_path TO public;")

        for script_name in SQL_SCRIPTS:
            script_path = os.path.join(sql_dir, script_name)
            print(f"Executando {script_path} ...")
            with open(script_path, "r", encoding="utf-8") as f:
                sql_code = f.read()
                # Separar múltiplos comandos
                commands = sql_code.split(";")
                for command in commands:
                    command = command.strip()
                    if command:
                        cur.execute(command)

        print("Banco de dados configurado com sucesso!")

    except Exception as e:
        print("Erro ao configurar banco de dados:", e)

    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()

def upsert_data():
    

if __name__ == "__main__":
    config_database()
