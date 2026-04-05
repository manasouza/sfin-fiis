
# SFin FIIs

Automatiza a coleta e registro de dados de Dividend Yield de Fundos Imobiliários (FIIs) brasileiros em uma planilha Google Sheets.

Baseado no PoC original: https://gist.github.com/manasouza/d2325b0bb7d4767f4942f5ab01ace9eb

## Modos de Operação

A aplicação suporta três modos de coleta de dados:

| Modo | Fonte | Descrição |
|------|-------|-----------|
| `webscraping` | fiis.com.br | Coleta via Scrapy spider com extração por XPath |
| `collected` | Dados externos | Recebe dados já coletados em formato JSON |
| `crewai` | investidor10.com.br | Busca via agentes CrewAI com LLM Perplexity Sonar |

## Estrutura

```
main_local.py           # CLI - ponto de entrada
fiis_workflow.py         # Workflows (Webscraping, Collected, CrewAI)
fiis.py                  # Implementação legada (scraping/collected)
config.yaml              # Configurações (layout planilha, XPath, CrewAI)
tools/
  gspreadsheet.py        # Integração Google Sheets (gspread)
  webscraping.py         # Scrapy spider para fiis.com.br
  crewai_search.py       # Agentes CrewAI + Perplexity + HuggingFace embeddings
test/
  fiis_workflow_test.py  # Testes unitários (validação CollectedDataWorkflow)
```

## Uso

```bash
# Modo webscraping
python main_local.py -m webscraping

# Modo collected (dados pré-coletados)
python main_local.py -m collected -f '{"XPML11": {"value": "0,85", "date": "15/03/2026"}}'

# Modo crewai (busca automática via IA)
python main_local.py -m crewai
```

## Variáveis de Ambiente

| Variável | Descrição |
|----------|-----------|
| `SPREADSHEET_ID` | ID da planilha Google Sheets |
| `CREDENTIALS_PATH` | Caminho para JSON da service account Google |
| `PERPLEXITY_API_KEY` | API key Perplexity (modo crewai) |

## Formato de Dados

```json
{
  "XPML11": {"value": "0,85", "date": "15/03/2026"},
  "VISC11": {"value": "1,10", "date": "10/03/2026"}
}
```

- **value**: formato moeda brasileira com vírgula (`X,XX`)
- **date**: formato `DD/MM/YYYY`, com limite de 30 dias

## Dependências

Principais: `scrapy`, `gspread`, `crewai`, `langchain-perplexity`, `sentence-transformers`

```bash
pip install -r requirements.txt
```
