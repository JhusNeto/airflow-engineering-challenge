# Airflow Engineering Challenge

Este repositório contém a solução para o desafio de Engenharia de Dados com Airflow. A solução utiliza Docker Compose para orquestrar os serviços (Airflow, PostgreSQL, API e Redis).

## Pré-requisitos

- **Docker Desktop**  
  Baixe e instale [Docker Desktop](https://www.docker.com/products/docker-desktop).  
  O Docker Desktop já inclui o Docker Engine e o Docker Compose (v2).  
- **Git**  
  Verifique se o Git está instalado:  
  ```bash
  git --version
   ```

## Setup do Ambiente

1. Clone o repositório ou faça fork
   ```bash
   git clone https://github.com/JhusNeto/airflow-engineering-challenge.git
   ```

2. Acesse o diretório do projeto
   ```bash
   cd airflow-engineering-challenge
   ```

3. Construir e subir os serviços
   ```bash
   docker compose up --build
   ```

4. Validar os Serviços
   
   - Containers Docker
   ```bash
   docker ps
   ```

## Comandos Úteis
- Parar os containers
   ```bash
   docker compose down
   ```
- Visualizar logs do container
   ```bash
   docker logs <container_id>
   ```
## Estrutura do Projeto
- `api`: Contém o código da API.
- `config`: Arquivos de configuração, incluindo os YAML dos endpoints da API.
- `dags`: Contém as DAGs do Airflow.
- `docs`: Contém os documentos do projeto.
- `init-scripts`: Contém os scripts de inicialização.
- `local_storage`: Armazena os dados brutos (Camada Raw).
- `logs`: Contém os logs do Airflow.
- `plugins`: Contém os plugins do Airflow.
- `.gitignore`: Contém os arquivos e diretórios que serão ignorados pelo Git.
- `data_flow.md`: Contém o diagrama de fluxo de dados do projeto.
- `docker-compose.yml`: Define os serviços e configurações do Docker Compose.
- `README.md`: Contém as instruções para configurar e executar o projeto.