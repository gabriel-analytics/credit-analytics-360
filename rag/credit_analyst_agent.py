"""
FinanceFlow Bank — Credit Analyst RAG Agent
Analista faz perguntas em linguagem natural, recebe insights dos dados via Claude API.
"""

import os
import json
import duckdb
import anthropic
from datetime import datetime

DB_PATH = "C:/Users/lineg/credit-analytics-360/gen/data/financeflow.duckdb"

# Schema summary injected as context
SCHEMA_CONTEXT = """
Você é um analista de crédito sênior do FinanceFlow Bank com acesso ao banco de dados DuckDB.

ESQUEMA DISPONÍVEL (schemas: main_staging, main_intermediate, main_marts):

main_marts.fct_credit_score
  - customer_id, credit_score (0-1000), risk_tier (very_low/low/medium/high/very_high)
  - alert_30d (bool), overall_default_rate, app_engagement_score
  - acquisition_channel, age_group, customer_segment, products_count

main_marts.fct_customer_ltv
  - customer_id, realized_ltv, projected_ltv_12m, ltv_segment, acquisition_channel

main_marts.fct_portfolio_health
  - mes (date), default_rate, npl_ratio, total_exposure, is_peak_month, total_active_contracts

main_marts.fct_collection_efficiency
  - channel_used, trigger_bucket (early/mid/late/bad), recovery_rate_pct, avg_resolution_days
  - is_best_window

main_marts.fct_risk_segments
  - customer_id, r_score, f_score, m_score, rfm_score, segment (champion/loyal/at_risk/lost/promising)
  - on_time_rate_pct, avg_amount_paid

main_intermediate.int_acquisition_quality
  - acquisition_channel, approval_rate_pct, default_rate_pct, avg_ltv, avg_ticket

main_intermediate.int_payment_performance
  - due_month, product_type, default_rate_pct, recovery_rate_pct, seasonal_index

INSIGHTS CONHECIDOS DO DATASET:
1. Inadimplência sobe ~31% em jan/fev (sazonalidade pós-festas)
2. paid_search tem 2.1x mais inadimplência que organic (9.73% vs 4.62%)
3. Clientes com 2+ produtos têm inadimplência 60% menor e LTV 4x maior
4. Queda >50% no uso do app nos 30 dias anteriores prediz inadimplência (73% acurácia)
5. vehicle_financing tem menor inadimplência (5.50%), personal_loan a maior (5.66%)
6. Contato no dia 1-7 de atraso: 65-70% recuperação. Após 30 dias: <25%
7. 565 clientes estão atualmente em alert_30d = true
8. Modelo ML: AUC=0.999, F1=0.824, Recall=0.973

REGRAS PARA GERAR SQL:
- Usar sempre schema qualificado: main_marts.fct_credit_score
- Limitar resultados a 20 linhas máximo
- Preferir ROUND() para métricas
- Não gerar DML (INSERT/UPDATE/DELETE)
"""

PRESET_QUESTIONS = [
    "Quais clientes têm maior risco de inadimplir nos próximos 30 dias?",
    "Qual canal de aquisição gera clientes com melhor LTV?",
    "Quando devo contatar clientes em atraso para maximizar recuperação?",
]


def query_db(sql: str) -> tuple[list[dict], str]:
    """Execute SQL on DuckDB and return rows + formatted table."""
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute(sql).df()
        con.close()
        rows = df.to_dict(orient="records")
        table = df.to_string(index=False, max_rows=20)
        return rows, table
    except Exception as e:
        return [], f"ERRO SQL: {e}"


def run_agent(question: str, verbose: bool = True) -> dict:
    """Run one RAG turn: question -> SQL -> data -> Claude insight."""

    client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

    # ── Step 1: Claude generates SQL ──────────────────────────────────────
    sql_prompt = f"""
{SCHEMA_CONTEXT}

PERGUNTA DO ANALISTA: {question}

Gere APENAS o SQL DuckDB para responder a pergunta.
Retorne somente o código SQL, sem explicações, sem markdown, sem ```sql.
O SQL deve ser executável diretamente.
"""

    sql_response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=800,
        messages=[{"role": "user", "content": sql_prompt}],
    )
    sql_query = sql_response.content[0].text.strip()

    # Clean common markdown artifacts
    sql_query = sql_query.replace("```sql", "").replace("```", "").strip()

    # ── Step 2: Execute SQL ────────────────────────────────────────────────
    rows, table_str = query_db(sql_query)

    # ── Step 3: Claude generates insight ──────────────────────────────────
    insight_prompt = f"""
{SCHEMA_CONTEXT}

PERGUNTA: {question}

SQL EXECUTADO:
{sql_query}

DADOS RETORNADOS:
{table_str}

Com base nos dados acima, gere uma resposta profissional em português com:
1. **Insight principal** (1-2 parágrafos): O que os dados revelam?
2. **Implicações de negócio**: O que isso significa para o FinanceFlow Bank?
3. **Ação recomendada**: Qual a próxima ação concreta para o time de crédito?

Seja objetivo, use os números reais dos dados. Máximo 300 palavras.
"""

    insight_response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1000,
        messages=[{"role": "user", "content": insight_prompt}],
    )
    insight = insight_response.content[0].text.strip()

    result = {
        "pergunta":   question,
        "sql":        sql_query,
        "n_rows":     len(rows),
        "dados":      rows[:10],
        "insight":    insight,
        "timestamp":  datetime.now().isoformat(),
    }

    if verbose:
        print()
        print("=" * 70)
        print(f"PERGUNTA: {question}")
        print("=" * 70)
        print()
        print("SQL GERADO:")
        print("-" * 40)
        print(sql_query)
        print()
        print(f"DADOS ({len(rows)} linhas):")
        print("-" * 40)
        print(table_str)
        print()
        print("INSIGHT:")
        print("-" * 40)
        print(insight)
        print("=" * 70)

    return result


def run_demo():
    """Run the 3 preset questions and save results."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("AVISO: ANTHROPIC_API_KEY não definida.")
        print("Defina com: export ANTHROPIC_API_KEY='sua-chave'")
        print()
        print("Rodando em modo DEMO (sem chamada API)...")
        _run_demo_local()
        return

    results = []
    for q in PRESET_QUESTIONS:
        try:
            r = run_agent(q)
            results.append(r)
        except Exception as e:
            print(f"Erro na pergunta '{q[:50]}...': {e}")

    # Save results
    with open("rag/agent_results.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nResultados salvos em rag/agent_results.json")


def _run_demo_local():
    """Demo without API key — shows SQL generation logic using DuckDB directly."""
    preset_sqls = [
        # Q1: Clientes com maior risco próximos 30 dias
        """SELECT customer_id, credit_score, risk_tier, overall_default_rate,
                  app_engagement_score, avg_days_late
           FROM main_marts.fct_credit_score
           WHERE alert_30d = true
           ORDER BY credit_score ASC
           LIMIT 10""",

        # Q2: Canal com melhor LTV
        """SELECT acquisition_channel,
                  ROUND(avg_ltv, 0) as ltv_medio,
                  ROUND(default_rate_pct, 2) as inadimplencia_pct,
                  total_customers
           FROM main_intermediate.int_acquisition_quality
           ORDER BY ltv_medio DESC""",

        # Q3: Melhor janela de cobrança
        """SELECT channel_used, trigger_bucket,
                  ROUND(recovery_rate_pct, 2) as recuperacao_pct,
                  ROUND(avg_resolution_days, 1) as dias_medio
           FROM main_marts.fct_collection_efficiency
           WHERE is_best_window = true
           ORDER BY recuperacao_pct DESC""",
    ]

    for q, sql in zip(PRESET_QUESTIONS, preset_sqls):
        rows, table = query_db(sql)
        print()
        print("=" * 70)
        print(f"PERGUNTA: {q}")
        print("=" * 70)
        print("\nSQL:")
        print(sql.strip())
        print(f"\nDADOS ({len(rows)} linhas):")
        print(table)
        print()

    demo_insights = {
        PRESET_QUESTIONS[0]: (
            "**565 clientes** estão em alerta de risco para os próximos 30 dias, "
            "com score médio de 312 e engajamento no app abaixo de 20. "
            "A queda no uso do app é o sinal mais precoce — os dados mostram "
            "correlação de 73% entre abandono do app e inadimplência futura.\n\n"
            "**Ação:** Acionar protocolo de cobrança preventiva imediata via SMS "
            "para todos os 565 clientes. Priorizar os 54 com risk_tier=very_high."
        ),
        PRESET_QUESTIONS[1]: (
            "Os canais **organic** e **referral** geram clientes com LTV médio "
            "de R$34.094 e R$34.151 respectivamente, com inadimplência de 4.62% "
            "e 4.58%. Já **paid_search** apresenta default de 9.73% — 2.1x maior — "
            "apesar de LTV similar.\n\n"
            "**Ação:** Redirecionar budget de marketing de paid_search para organic "
            "e referral. ROI ajustado ao risco é significativamente superior."
        ),
        PRESET_QUESTIONS[2]: (
            "A janela **early (1-7 dias)** é criticamente superior: taxa de "
            "recuperação de **65-70%** via letter/email vs apenas **20%** após 30 dias. "
            "Todos os 5 canais analisados têm sua melhor janela no período early.\n\n"
            "**Ação:** Implementar automação de cobrança D+1 com SMS, D+3 com WhatsApp "
            "e D+5 com ligação. Eliminar delays burocráticos que empurram contatos para "
            "a janela mid/late onde a recuperação cai pela metade."
        ),
    }

    print("\n" + "=" * 70)
    print("INSIGHTS PRÉ-COMPUTADOS (modo demo — sem API key):")
    print("=" * 70)
    for q, insight in demo_insights.items():
        print(f"\n> {q}")
        print(insight)

    print("\n" + "=" * 70)
    print("Para rodar com Claude API real:")
    print("  $env:ANTHROPIC_API_KEY='sua-chave'  (PowerShell)")
    print("  python rag/credit_analyst_agent.py")
    print("=" * 70)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        # Custom question from CLI: python rag/credit_analyst_agent.py "Sua pergunta"
        q = " ".join(sys.argv[1:])
        run_agent(q)
    else:
        run_demo()
