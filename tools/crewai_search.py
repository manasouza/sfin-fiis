import os
import json
import logging
import random
import time

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


def _crew_outputs(results):
    for result in results:
        if isinstance(result, list):
            yield from result
        else:
            yield result


def _request_delay(max_rpm: int) -> float:
    if not max_rpm or max_rpm <= 0:
        return 0
    return 60 / max_rpm


def search_fii_dividends(fiis_list: list, website_url: str, model: str = 'perplexity/sonar', max_rpm: int = 10) -> dict:
    """
    Search for FII dividend data using CrewAI agents with Perplexity LLM.

    Args:
        fiis_list: List of FII ticker codes to search for
        website_url: Base URL of the financial website to search
        model: LLM model identifier for Perplexity
        max_rpm: Maximum requests per minute for the crew

    Returns:
        Dictionary mapping FII codes to their dividend data {'value': ..., 'date': ...}
    """
    import litellm
    from crewai import Agent, Task, Crew, LLM
    from crewai_tools import WebsiteSearchTool
    from litellm.exceptions import BadRequestError

    os.environ.setdefault("CREWAI_DISABLE_TELEMETRY", "true")
    os.environ.setdefault("CREWAI_TRACING_ENABLED", "false")
    os.environ.setdefault("OTEL_SDK_DISABLED", "true")
    os.environ.setdefault("OPENAI_API_KEY", "NA")

    litellm.num_retries = 3
    litellm.retry_after = 5

    perplexity_api_key = os.environ.get("PERPLEXITY_API_KEY")
    if not perplexity_api_key:
        raise ValueError("PERPLEXITY_API_KEY environment variable is required")

    rag_search_tool = WebsiteSearchTool(
        config=dict(
            llm=dict(
                provider="openai",
                config=dict(
                    model=model,
                    api_key=perplexity_api_key,
                    base_url="https://api.perplexity.ai"
                ),
            ),
            embedder=dict(
                provider="huggingface",
                config=dict(
                    model="sentence-transformers/all-MiniLM-L6-v2",
                ),
            ),
        )
    )

    perplexity_llm = LLM(
        model=model,
        api_key=perplexity_api_key,
        base_url="https://api.perplexity.ai"
    )

    finance_analyst = Agent(
        role='Analista financeiro',
        goal='Descobrir o valor do dividendo mensal de Fundos Imobiliários (FIIs)',
        backstory=(
            "Como Analista Financeiro que atende alguns clientes, "
            "preciso dar informações dos FIIs de minha carteira recomendada"
        ),
        llm=perplexity_llm
    )

    fii_dy_search = Task(
        description=(
            "Procurar no site " + website_url + " a informação do valor do dividendo mais recente do FII {fii_code}. "
            "Seja sucinto, não adicione análises ou resumos sobre os elementos encontrados. "
            "Cada FII tem sua página (fiis/{fii_code}). Considerar somente a seção com título {fii_code} DIVIDENDOS."
            "Esta mesma seção demonstra as Distribuições nos últimos 12 meses. Considerar somente o registro da primeira linha da tabela."
            "Extraia somente a 'Data Base' (geralmente denominada 'Data Com') e o valor do dividendo, normalmente referenciado como 'Valor' ou 'Rendimento'."
            "Geralmente existe outra data definida como 'Data de pagamento', mas esta deve ser descartada. Considerar 'Data Com' como válida. Para ajudar a decidir, a 'Data Com' é sempre anterior à 'Data de pagamento'."
            "Ademais, a 'Data Com' a ser definida tem que ser no máximo referente ao mês anterior."
        ),
        expected_output=(
            "Sem texto explicativo. "
            "Objeto JSON sendo o código de FII como chave, que contém a Data de fechamento no atributo date, "
            "o valor do dividendo no atributo value. "
            "Equalize o formato de data como dd/MM/yyyy. "
            "Equalize o formato de valor como string, separado por vírgula."
        ),
        agent=finance_analyst,
        tools=[rag_search_tool],
    )

    inputs = [{'fii_code': fii} for fii in fiis_list]
    logging.info(f'Starting CrewAI search for {len(fiis_list)} FIIs: {fiis_list}')

    results = []
    failed = []
    delay_seconds = _request_delay(max_rpm)

    for index, fii_input in enumerate(inputs, start=1):
        crew = Crew(
            agents=[finance_analyst],
            tasks=[fii_dy_search],
            max_rpm=max_rpm,
            memory=False,
            tracing=False,
            verbose=0,
        )
        fii_code = fii_input['fii_code']
        try:
            response = crew.kickoff_for_each(inputs=[fii_input])
            results.append(response)
            logging.info(f'[{index}/{len(inputs)}] CrewAI search completed for {fii_code}')
        except BadRequestError as e:
            logging.warning(f'[{index}/{len(inputs)}] CrewAI BadRequest for {fii_code}: {e}')
            failed.append(fii_code)
        except Exception as e:
            logging.warning(f'[{index}/{len(inputs)}] CrewAI search failed for {fii_code}: {e}')
            failed.append(fii_code)

        if index < len(inputs) and delay_seconds:
            time.sleep(delay_seconds + random.uniform(0, min(2, delay_seconds * 0.2)))

    fiis_data = {}
    for res in _crew_outputs(results):
        raw_result = getattr(res, 'raw', '')
        try:
            json_str = raw_result.replace('json\n', '').replace('\n', '').replace('`', '').strip()
            fii_json = json.loads(json_str)
            fiis_data.update(fii_json)
        except (json.JSONDecodeError, AttributeError) as e:
            logging.warning(f'Failed to parse CrewAI result: {raw_result} - Error: {e}')

    if failed:
        logging.warning(f'CrewAI search failed for {len(failed)} FIIs: {failed}')
    logging.info(f'CrewAI search completed. Found data for {len(fiis_data)} FIIs')
    return fiis_data
