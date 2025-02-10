```mermaid
graph TD
    %% Extracao e Orquestracao
    subgraph Extracao
      API[API Endpoints]
      Auth[Autenticacao JWT e Refresh]
      Pagina[Paginacao e Tratamento de Erros]
      YAML[YAML Config Files]
      API --> Auth
      Auth --> Pagina
      Pagina --> DAG[Airflow DAG]
      YAML --> DAG
    end

    %% Processamento dos Dados
    subgraph Processamento
      Raw[Camada Raw - Dados Brutos]
      Stage[Camada Stage - Normalizacao]
      Trusted[Camada Trusted - Agregacoes]
      DAG --> Raw
      Raw --> Stage
      Stage --> Trusted
    end

    %% Opcional com DBT
    subgraph Opcional
      DBT[DBT Modelos e Testes]
      Stage --> DBT
      DBT --> Trusted
    end