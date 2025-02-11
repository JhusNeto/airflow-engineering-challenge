-- Criação do schema stage para armazenar tabelas intermediárias
-- Este schema é usado como uma área de preparação dos dados antes de serem carregados
-- para a camada trusted
CREATE SCHEMA IF NOT EXISTS stage;