# Teste Técnico – Engenharia de Dados com Airflow

## Objetivo do Teste

Avaliar a capacidade técnica do candidato em:

1. **Construir pipelines de dados** utilizando Airflow.  
2. **Resolver problemas** comuns de integração (autenticação de API, paginação, reintentos, etc.).  
3. **Organização de dados** em camadas (Raw, Stage e Trusted).  
4. **Boas práticas** de versionamento (Git), documentação e containerização (Docker).

## Requisitos do Sistema

1. **Hardware Mínimo**
   - CPU: 2 cores
   - RAM: 8GB
   - Disco: 30GB livres

2. **Software Necessário**
   - Docker Engine 20.10+
   - Docker Compose v2.0+
   - Git

3. **Portas Necessárias**
   - 8080: Airflow Webserver
   - 8000: API
   - 5432: PostgreSQL
      - airflow: Banco de Dados do Airflow (Não USAR)
      - api: Banco de Dados da API (Não USAR)
      - ecommerce: Banco de Dados para Inserir os dados trabalhados

## O que Será Avaliado

1. **Qualidade e Organização do Código**  
   - Legibilidade, estrutura de arquivos, uso de funções ou classes para evitar repetição de código.  
   - Documentação no código e comentários claros.

2. **Documentação e Configurações**  
   - Uso de variáveis do Airflow, conexões e configurações externas (evitar valores "hard-coded").  
   - README e instruções para rodar o projeto.

3. **Resiliência e Tratamento de Erros**  
   - Como o candidato lida com autenticação JWT (token curto + refresh).  
   - Retry de requests em caso de erros 500, rate-limit e demais inconsistências.

4. **Organização dos Dados**  
   - Estrutura das tabelas/arquivos nas camadas Raw, Stage e Trusted.  
   - Transformações e sumarizações na camada Trusted, pensando em uso de negócio.

5. **Gerenciamento de Containers**  
   - Uso do Docker Compose para subir os serviços (API, Postgres, Airflow, etc.).  
   - Familiaridade com logs e troubleshooting básico de containers.

6. **Boas Práticas de Git**  
   - Commits periódicos e semânticos (mensagens claras).  
   - Estrutura de branch e pull requests/fork, se aplicável.

## Como Funciona

1. **Acesso ao Repositório**  
   - Faça um **fork** desse repositório para a sua conta Git (ou um clone privado, conforme instrução).

2. **Setup Local**  
   - Suba todos os serviços em sua máquina usando `docker-compose up --build`.  
   - Verifique se a API e o Airflow estão funcionando corretamente.

3. **Desenvolvimento da Solução**  
   - Crie uma ou mais DAGs no Airflow para **consumir os dados da API** e armazenar nas camadas descritas (Raw, Stage e Trusted).  
   - Realize **commits periódicos** e com mensagens descritivas.  
   - Parametrize tudo o que for necessário em variáveis do Airflow ou em configurações (YAML, `.env`, etc.).

4. **Entrega**  
   - Finalizada a implementação, disponibilize o repositório (fork) com seu código.  
   - Inclua um README explicando como rodar, principais componentes e decisões técnicas adotadas.

## O que é Esperado

1. **Consumir Endpoints da API** e Armazenar em 3 Estágios:
   - **Raw**  
     - Dados brutos, no formato JSON ou Parquet
     - Estrutura esperada dos arquivos:
       ```
       local_storage/
       ├── raw/
       │   ├── products/
       │   │   └── YYYY-MM-DD/
       │   │       └── products_YYYYMMDD_HHMMSS.json
       │   ├── carts/
       │   └── customers/
       ```
   - **Stage** (em banco de dados)  
     - Tabelas intermediárias, com dados "explodidos" (evitando colunas do tipo JSON ou listas)
     - Estrutura sugerida das tabelas:
       ```sql
       -- Exemplo para products
       CREATE TABLE stage.products (
           id INTEGER PRIMARY KEY,
           name VARCHAR(255),
           price DECIMAL(10,2),
           category VARCHAR(100),
           created_at TIMESTAMP,
           updated_at TIMESTAMP
       );
       ```
   - **Trusted** (em banco de dados)  
     - Tabelas **criadas e pensadas para relatório**, **sumarizadas** e prontas para consumo analítico
     - Exemplo de agregações esperadas:
       ```sql
       -- Exemplo de visão agregada
       CREATE TABLE trusted.product_sales_daily (
           date DATE,
           category VARCHAR(100),
           total_sales DECIMAL(10,2),
           avg_ticket DECIMAL(10,2),
           num_transactions INTEGER
       );
       ```

2. **Estrutura e Padrões da DAG**  
   - Cada endpoint (por exemplo, `products`, `carts`, `customer`, `logistict`) deve ser **definido em um arquivo YAML** (ou em um YAML “master”), onde se descrevem parâmetros de consumo (URL, rotas, limite de paginação, etc.).  
     Exemplo:
     ```resources:
      customer:                               
         endpoint: "/customer"                   
         file_name: customer                   
         parse_point: ""                       
         limit: 50                              
         table_name: tb_customers```
   - A DAG deve ler esse YAML e **gerar dinamicamente** um _task_group_ (ou tasks individuais) para cada endpoint.  
   - **Evitar** repetição de código: crie funções ou classes que possam ser reutilizadas para cada endpoint.  
   - Qualquer parâmetro (URL-base, caminhos de arquivo, horários de execução, tokens) deve ser preferencialmente passado via **Variáveis do Airflow** ou configurações externas.
   - Usar o dbt será um diferencial

4. **Uso do DBT (Diferencial)**
   - Se optar por usar DBT, criar models para as camadas Stage e Trusted
   - Documentar a linhagem dos dados
   - Implementar testes de qualidade de dados

## Observações Importantes

1. **Tokens e Refresh**  
   - A API exige login (`POST /token`) com `username=admin` e `password=admin`
   - O **token expira em 30 minutos**, portanto, implemente refresh quando necessário (`POST /refresh-token`)

2. **Erros 500 Aleatórios**  
   - A API pode retornar `500 Internal Server Error` em algumas chamadas
   - Esperamos ver **retentativas automáticas** (com backoff, por exemplo)

3. **Paginação**  
   - Use `skip` e `limit` para coletar todos os registros. O `limit` máximo é 50
   - A DAG deve iterar até não haver mais dados

4. **Commits**  
   - Faça commits com mensagens descritivas (ex.: "fix: corrigindo lógica de token refresh" ou "feat: adiciona task de load na camada Trusted")
   - Isso nos ajuda a entender seu processo de desenvolvimento

## Entregável

- **Repositório Git** (seu fork) com:  
  1. **DAG(s)** criadas
  2. **Arquivo(s) YAML** de definição dos endpoints
  3. **README** documentando a estrutura e explicando como rodar o projeto
  4. Scripts auxiliares (se necessários) bem organizados e referenciados no README

**Ao concluir**, envie o link do seu repositório para o avaliador.
