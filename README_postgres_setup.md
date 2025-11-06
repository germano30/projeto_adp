# Configuração Completa do PostgreSQL 16 com pgvector e Apache AGE

## 1. Introdução

Este documento descreve o processo completo de instalação e configuração do **PostgreSQL 16** em ambiente Linux (Debian/Ubuntu), incluindo as extensões **pgvector** e **Apache AGE**.  

Essas extensões permitem o uso de **vetores** para aplicações de aprendizado de máquina e de **grafos** para modelagem relacional avançada, respectivamente.

---

## 2. Remoção de versões anteriores

Antes de iniciar a instalação, é recomendável remover eventuais versões anteriores do PostgreSQL para garantir um ambiente limpo.

```bash
sudo systemctl stop postgresql
sudo apt purge -y postgresql* libpq-dev
sudo rm -rf /etc/postgresql /var/lib/postgresql /usr/lib/postgresql
sudo apt autoremove -y
sudo apt autoclean
```

---

## 3. Instalação do PostgreSQL 16

Atualize o sistema e adicione o repositório oficial do PostgreSQL:

```bash
sudo apt update
sudo apt install -y curl ca-certificates gnupg lsb-release
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg
echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list
sudo apt update
```

Em seguida, instale o PostgreSQL 16 e suas extensões:

```bash
sudo apt install -y postgresql-16 postgresql-16-pgvector postgresql-16-age
```

---

## 4. Inicialização do serviço

Inicie o serviço do PostgreSQL e verifique o status:

```bash
sudo systemctl start postgresql
sudo systemctl status postgresql
```

---

## 5. Configuração para o LightRag

### 5.1 Criação do usuário e do banco de dados

Acesse o shell do PostgreSQL com o usuário padrão:

```bash
sudo -u postgres psql
```

Dentro do console, execute:

```sql
CREATE USER <your_user> WITH PASSWORD '<password>';
CREATE DATABASE <database> OWNER <your_user>;
GRANT ALL PRIVILEGES ON DATABASE <database> TO <your_user>;
\q
```

### 5.2 Ativação das extensões

Conecte-se ao banco de dados recém-criado como superusuário e ative as extensões:

```bash
sudo -u postgres psql -d <database> -c "CREATE EXTENSION IF NOT EXISTS vector;"
sudo -u postgres psql -d <database> -c "CREATE EXTENSION IF NOT EXISTS age;"
sudo -u postgres psql -d <database> -c "LOAD 'age';"
sudo -u postgres psql -d <database> -c "SET search_path = ag_catalog, \"\$user\", public;"
```

Após isso, teste se o `search_path` está correto:

```bash
sudo -u postgres psql -d <database> -c "SHOW search_path;"
```

O resultado esperado:

```
        search_path         
-----------------------------
 ag_catalog, "$user", public
```

Caso não apareça assim, faça esse comando:

```bash
sudo -u postgres psql -d <database> -c "ALTER DATABASE <database> SET search_path = ag_catalog, \"\$user\", public;"  
```

Agora entre como superusuário e garanta que o usuário criado tenha todas as permissões:

```bash
sudo -u postgres psql -d <database>
```

```sql
GRANT USAGE ON SCHEMA ag_catalog TO <your_user>;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA ag_catalog TO <your_user>;
\q
```

### 5.3 Configuração de variáveis de ambiente

No arquivo `.env`, substitua as variáveis:

```
POSTGRES_USER=<your_user>
POSTGRES_PASSWORD=<password>
POSTGRES_DATABASE=<database>
```

Para os valores que você criou acima.

### 5.4 Teste de funcionamento

Para confirmar que as extensões foram instaladas corretamente:

```bash
psql -U <your_user> -d <database> -c "\dx"
```

O resultado deve listar algo semelhante a:

```
   Name   | Version |   Schema   |              Description              
----------+---------+------------+---------------------------------------
 age      | 1.4.0   | ag_catalog | graph database extension for PostgreSQL
 pgvector | 0.5.0   | public     | vector data type for machine learning
```

### 5.5 Referências

- Documentação oficial do PostgreSQL: https://www.postgresql.org/docs/16/
- Repositório do Apache AGE: https://github.com/apache/age
- Repositório do pgvector: https://github.com/pgvector/pgvector

---

## 6. Configuração para uso SQL padrão

Esta seção descreve como criar um banco de dados e um usuário SQL padrão, sem privilégios administrativos, para uso em aplicações ou ambientes de desenvolvimento.

### 6.1 Criação de um novo usuário SQL

Acesse o shell do PostgreSQL como o superusuário `postgres`:

```bash
sudo -u postgres psql
```

Dentro do console, execute:

```sql
CREATE USER <sql_user> WITH PASSWORD '<sql_password>';
CREATE DATABASE <sql_database> OWNER <sql_user>;
GRANT ALL PRIVILEGES ON DATABASE <sql_database> TO <sql_user>;
\q
```

### 6.2 Conexão com o banco de dados

Teste a conexão com o novo usuário:

```bash
psql -U <sql_user> -d <sql_database> -h localhost
```

Você será solicitado a inserir a senha. Após autenticação bem-sucedida, você terá acesso ao banco de dados.

### 6.3 Configuração de permissões adicionais (opcional)

Caso precise conceder permissões específicas em schemas ou tabelas:

```bash
sudo -u postgres psql -d <sql_database>
```

```sql
GRANT ALL PRIVILEGES ON SCHEMA public TO <sql_user>;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO <sql_user>;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO <sql_user>;
\q
```

### 6.4 Teste de criação de tabela

Conecte-se novamente como o usuário criado e teste a criação de uma tabela:

```bash
psql -U <sql_user> -d <sql_database> -h localhost
```

```sql
CREATE TABLE teste (
    id SERIAL PRIMARY KEY,
    nome VARCHAR(100),
    criado_em TIMESTAMP DEFAULT NOW()
);

INSERT INTO teste (nome) VALUES ('Teste de funcionamento');

SELECT * FROM teste;

DROP TABLE teste;

\q
```

O resultado deve exibir a linha inserida, confirmando que o usuário tem permissões adequadas.

### 6.5 Configuração para aplicações

Para conectar sua aplicação ao banco de dados, utilize a string de conexão:

```
postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_DATABASE}
```

Ou configure as variáveis de ambiente:

```
DB_USER=<sql_user>
DB_PASSWORD=<sql_password>
DB_HOST=localhost
DB_PORT=5432
DB_NAME=<sql_database>
```

---

## 7. Comandos úteis

### Verificar versão do PostgreSQL
```bash
psql --version
```

### Listar bancos de dados
```bash
sudo -u postgres psql -c "\l"
```

### Listar usuários
```bash
sudo -u postgres psql -c "\du"
```

### Conectar a um banco específico
```bash
psql -U <usuario> -d <banco> -h localhost
```

### Backup de um banco de dados
```bash
pg_dump -U <usuario> -d <banco> -F c -f backup.dump
```

### Restaurar backup
```bash
pg_restore -U <usuario> -d <banco> backup.dump
```

### Reiniciar o serviço PostgreSQL
```bash
sudo systemctl restart postgresql
```

---

## 8. Solução de problemas comuns

### Erro de autenticação
Se você receber erros de autenticação, verifique o arquivo `pg_hba.conf`:

```bash
sudo nano /etc/postgresql/16/main/pg_hba.conf
```

Certifique-se de que a linha para conexões locais esteja configurada como:

```
local   all             all                                     md5
host    all             all             127.0.0.1/32            md5
```

Após modificar, reinicie o serviço:

```bash
sudo systemctl restart postgresql
```

### Extensão não encontrada
Se a extensão não for reconhecida, verifique se o pacote está instalado:

```bash
apt list --installed | grep postgresql-16
```

E reinstale se necessário:

```bash
sudo apt install -y postgresql-16-pgvector postgresql-16-age
```

---

## 9. Conclusão

Este guia fornece uma configuração completa do PostgreSQL 16 com suporte a vetores e grafos, além de configurações básicas para uso em aplicações SQL convencionais. Para uso avançado, consulte a documentação oficial mencionada nas referências.