import os
import json
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)


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

    os.environ.setdefault("CREWAI_TRACING_ENABLED", "false")
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
                    model="perplexity/sonar",
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
            "Cada FII tem sua página (fiis/{fii_code}). Considerar somente a seção com título {fii_code} DIVIDENDOS, "
            "sob Distribuições nos últimos 12 meses. "
            "Considerar somente o registro da primeira linha da tabela. "
            "Extraia somente a Data Base (Data Com) e o valor do dividendo, normalmente referenciado como Valor or Rendimento."
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

    crew = Crew(
        agents=[finance_analyst],
        tasks=[fii_dy_search],
        verbose=True,
        tracing=False,
        max_rpm=max_rpm
    )

    inputs = [{'fii_code': fii} for fii in fiis_list]
    logging.info(f'Starting CrewAI search for {len(fiis_list)} FIIs: {fiis_list}')
    results = crew.kickoff_for_each(inputs=inputs)

    fiis_data = {}
    for res in results:
        try:
            json_str = res.raw.replace('json\n', '').replace('\n', '').replace('`', '').strip()
            fii_json = json.loads(json_str)
            fiis_data.update(fii_json)
        except (json.JSONDecodeError, AttributeError) as e:
            logging.warning(f'Failed to parse CrewAI result: {res.raw} - Error: {e}')

    logging.info(f'CrewAI search completed. Found data for {len(fiis_data)} FIIs')
    return fiis_data
