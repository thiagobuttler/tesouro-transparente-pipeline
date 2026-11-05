# Pipeline Tesouro Direto

Projeto individual de engenharia de dados que transforma o histórico de cotações do Tesouro Direto (dataset público com 3,7 milhões de registros, desde 2004) em uma base analítica no Databricks, organizada em camadas bronze, silver e gold, com governança via Unity Catalog.

## Objetivo

Construir um pipeline de dados completo, de ponta a ponta, e mostrar como eu organizo um projeto antes de escrever código: cada pasta e cada arquivo com uma responsabilidade única, configuração externalizada, validações antes de gravar os dados e tratamento de erros com exceções específicas. O resultado é um conjunto de tabelas analíticas prontas para consulta em SQL e um dashboard com curvas de juros, inflação implícita e risco por título.

## Arquitetura

```text
Tesouro Transparente (CSV público)
        |
        |  AWS Lambda: baixa o CSV com pandas e grava Parquet no S3
        v
S3 bucket "tesouro-transparente-pipeline"
        |
        |  Volume externo do Unity Catalog (acesso ao S3)
        v
BRONZE: dados brutos em Parquet
        |
        |  src/main.py no Databricks (PySpark, compute serverless)
        v
SILVER: dados limpos e padronizados
        |
        v
GOLD: tabelas analíticas (dimensões e fatos)
```

O pipeline é batch e idempotente: todas as tabelas são sobrescritas a cada execução. Antes de gravar a camada silver, um check de data quality coleta métricas como taxas fora do range esperado, PUs negativos, nulos em campos críticos e duplicatas, e registra o resultado no log.

## Estrutura do projeto

```text
src/
├── main.py                 # ponto de entrada: logging, injeção de dependências e tratamento de erros
├── extract/
│   └── extractor.py        # Lambda na AWS que baixa o CSV e grava no S3
├── io_utils/
│   ├── data_handler.py     # leitura e escrita de dados (única fronteira com o storage)
│   ├── validator.py        # valida o ambiente antes de executar (volume bronze e schemas)
│   └── exceptions.py       # exceções específicas do pipeline
├── config/
│   ├── settings.py         # carrega o settings.yaml
│   └── settings.yaml       # caminhos e logging externalizados
├── session/
│   └── spark_session.py    # registro didático: a sessão Spark é injetada pelo runtime do Databricks
├── processing/
│   └── transformations.py  # regras de negócio (recebe e devolve DataFrames, sem I/O)
├── quality/
│   └── quality.py          # validações de qualidade de dados sobre a camada silver
└── pipeline/
    └── pipeline.py         # orquestração: sequência bronze, silver e gold

tests/
├── unit/                   # testes unitários (em construção)
└── integration/            # testes de integração (em construção)

notebooks/
└── exploracao              # análises exploratórias

requirements.txt            # dependências: pyyaml (execução) e pytest, ruff, black (desenvolvimento)
```

A ideia central da divisão: transformations não sabe ler nem gravar dados, data_handler não aplica regra de negócio, quality só coleta métricas de qualidade e main.py não conhece caminhos de tabela. Assim, uma regra pode ser testada isoladamente, e trocar um caminho é editar YAML, não código.

## Tabelas criadas em cada camada

| Camada | Tabela | Conteúdo |
| --- | --- | --- |
| Bronze | arquivos Parquet no Volume `/Volumes/tesouro_transparente_pipeline/bronze/raw_data` | dados brutos, um arquivo por extração da Lambda |
| Silver | `tesouro_transparente_silver` | dados limpos: colunas renomeadas e datas no tipo correto |
| Silver | `stg_zero_cupom` | base intermediária com apenas Prefixado e IPCA+, com deduplicação e taxa média |
| Gold | `dim_titulo` | dimensão de títulos: tipo, vencimento e indexador |
| Gold | `dim_data` | calendário contínuo de 2004 até hoje, com marcação dos dias com cotação |
| Gold | `fct_precos_taxas` | preços e taxas de compra e venda por título e data |
| Gold | `fct_curva_juros_vertices` | curva de juros por família em vértices de 1, 2, 3, 5 e 10 anos |
| Gold | `fct_inflacao_implicita` | expectativa de inflação embutida nos preços (relação Fisher) |
| Gold | `fct_risco_titulo` | métricas de risco por título: duration, DV01, volatilidade e drawdown |
| Gold | `fct_sinais_mercado` | compara a taxa atual com a própria história (z-score e percentil) |
| Gold | `fct_inclinacao_curva` | inclinação da curva de juros (2s5s e 2s10s), com marcação de curva invertida |

## Quais análises as tabelas gold permitem

- Evolução das curvas de juros prefixada e IPCA+ ao longo de mais de 20 anos, por vértice.
- A expectativa de inflação implícita no mercado, obtida comparando a curva prefixada com a curva IPCA+.
- Ranking de risco por título, com duration, DV01, volatilidade e drawdown.
- Leitura de mercado: se a taxa atual está no topo ou no piso da distribuição histórica das janelas de 1, 3 e 5 anos.
- Formato da curva: se está inclinada para cima, plana ou invertida.

## Como executar

1. Ingestão: rodar a Lambda `src/extract/extractor.py` na AWS, que grava os dados brutos no S3, acessíveis no Databricks via Volume do Unity Catalog.
2. Pipeline: executar `src/main.py` em compute serverless no Databricks (a sessão Spark é injetada pelo runtime).
3. Consumo: consultar as tabelas do catálogo `tesouro_transparente_pipeline.gold` em SQL ou o dashboard "Tesouro Direto, Análise das Taxas Históricas".

## Resultados

- 3,7 milhões de registros processados em execução de cerca de 2 minutos em compute serverless, a um custo aproximado de US$ 1 por execução.
- 8 tabelas gold cobrindo 5.439 datas base, de 2004 até hoje.
- Dashboard analítico sobre as tabelas gold, com curvas por família, inflação implícita, sinais de mercado e ranking de volatilidade.

## Limitações e próximos passos

- Carga batch com sobrescrita total; a evolução natural é carga incremental.
- A Lambda de ingestão é executada manualmente e não tem infraestrutura como código versionada.
- Testes automatizados em construção: as pastas tests/unit e tests/integration já existem, com pytest e pytest-cov no requirements.txt, mas ainda não há casos de teste escritos.
- `main.py` depende da variável `spark` injetada pelo runtime do Databricks e não roda fora dele.

## Stack

Python, PySpark, Databricks (Serverless, Unity Catalog, Delta Lake), AWS (Lambda, S3), pandas. Ferramentas de desenvolvimento: pytest, ruff e black.

## Autor

Projeto individual de estudante de Engenharia de Dados (FIAP).
[LinkedIn](link) | [GitHub](link)
