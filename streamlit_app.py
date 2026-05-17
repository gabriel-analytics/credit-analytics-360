import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import duckdb
import pandas as pd

st.set_page_config(
    page_title="Credit Analytics 360° — FinanceFlow Bank",
    page_icon="🏦",
    layout="wide"
)

DB = "C:/Users/lineg/credit-analytics-360/gen/data/financeflow.duckdb"

@st.cache_data
def query(sql):
    con = duckdb.connect(DB, read_only=True)
    df = con.execute(sql).df()
    con.close()
    return df

st.title("🏦 Credit Analytics 360°")
st.caption("FinanceFlow Bank | Analytics Engineering Case | Gabriel Pacheco")
st.divider()

tab1, tab2, tab3, tab4 = st.tabs([
    "📊 Portfolio Overview",
    "⚠️ Risk Analytics",
    "👥 Customer Segments",
    "🤖 ML Predictions"
])

# ABA 1 — PORTFOLIO OVERVIEW
with tab1:
    kpis = query("""
        SELECT
            (SELECT count(distinct customer_id)
             FROM main_marts.fct_credit_score) AS total_clientes,
            (SELECT round(total_exposure / 1e9, 2)
             FROM main_marts.fct_portfolio_health
             ORDER BY mes DESC LIMIT 1) AS carteira_bi,
            (SELECT round(avg(default_rate), 1)
             FROM main_marts.fct_portfolio_health
             WHERE total_active_contracts > 1000) AS inadimplencia_pct,
            (SELECT count(*)
             FROM main_marts.fct_credit_score
             WHERE alert_30d = true) AS alertas_30d
    """)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Clientes Ativos", f"{int(kpis['total_clientes'][0]):,}")
    col2.metric("Carteira Total", f"R$ {kpis['carteira_bi'][0]:.2f}B")
    col3.metric("Inadimplência Média", f"{kpis['inadimplencia_pct'][0]:.1f}%")
    col4.metric("Alertas 30d", f"{int(kpis['alertas_30d'][0]):,}")

    st.subheader("Inadimplência Mensal")
    ph = query("""
        SELECT
            strftime(mes::DATE, '%Y-%m') AS mes_str,
            default_rate AS default_pct,
            is_peak_month
        FROM main_marts.fct_portfolio_health
        WHERE total_active_contracts > 1000
        ORDER BY mes
    """)
    ph['cor'] = ph['is_peak_month'].map({True: 'Jan/Fev (Pico)', False: 'Normal'})
    fig1 = px.line(ph, x='mes_str', y='default_pct',
                   title='Default Rate Mensal (%)',
                   labels={'mes_str': 'Mês', 'default_pct': 'Default Rate (%)'},
                   color='cor',
                   color_discrete_map={'Jan/Fev (Pico)': '#ef4444', 'Normal': '#3b82f6'})
    st.plotly_chart(fig1, use_container_width=True)
    st.info("💡 **Insight:** Pico sazonal em Jan/Fev — inadimplência +29% vs média anual (7.2% vs 5.5%)")

    st.subheader("Default Rate por Produto")
    prod = query("""
        SELECT product_type,
            round(avg(default_rate_pct), 2) AS default_pct
        FROM main_intermediate.int_payment_performance
        GROUP BY product_type
        ORDER BY default_pct DESC
    """)
    fig2 = px.bar(prod, x='product_type', y='default_pct',
                  color='default_pct',
                  color_continuous_scale='RdYlGn_r',
                  title='Default Rate por Produto (%)')
    st.plotly_chart(fig2, use_container_width=True)

# ABA 2 — RISK ANALYTICS
with tab2:
    alertas_count = query("""
        SELECT count(*) AS total
        FROM main_marts.fct_credit_score
        WHERE alert_30d = true
    """)
    st.metric("🚨 Clientes em Alerta (próximos 30 dias)",
              f"{int(alertas_count['total'][0]):,}")
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Distribuição por Risk Tier")
        tiers = query("""
            SELECT risk_tier,
                count(*) AS clientes,
                round(count(*) * 100.0 / sum(count(*)) OVER (), 1) AS pct
            FROM main_marts.fct_credit_score
            GROUP BY risk_tier
            ORDER BY clientes DESC
        """)
        fig3 = px.bar(tiers, x='risk_tier', y='clientes',
                      color='risk_tier',
                      color_discrete_map={
                          'very_low': '#22c55e',
                          'low': '#86efac',
                          'medium': '#fbbf24',
                          'high': '#f97316',
                          'very_high': '#ef4444'
                      },
                      title='Clientes por Risk Tier')
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        st.subheader("Risco por Canal de Aquisição")
        canal = query("""
            SELECT acquisition_channel,
                round(default_rate_pct, 2) AS default_pct,
                round(avg_ltv, 0) AS ltv_medio,
                total_customers
            FROM main_intermediate.int_acquisition_quality
            ORDER BY default_pct DESC
        """)
        fig4 = px.bar(canal, x='acquisition_channel', y='default_pct',
                      color='default_pct',
                      color_continuous_scale='RdYlGn_r',
                      title='Default Rate por Canal (%)')
        st.plotly_chart(fig4, use_container_width=True)

    st.subheader("Top 20 Clientes em Alerta")
    alertas = query("""
        SELECT
            customer_id,
            round(credit_score, 0) AS score,
            risk_tier,
            round(app_engagement_score, 1) AS engagement
        FROM main_marts.fct_credit_score
        WHERE alert_30d = true
        ORDER BY credit_score ASC
        LIMIT 20
    """)
    st.dataframe(alertas, use_container_width=True)
    st.warning("⚠️ **Insight:** Canal paid_search tem 9.73% de default vs organic com menor risco e maior LTV")

# ABA 3 — CUSTOMER SEGMENTS
with tab3:
    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Segmentos RFM de Crédito")
        segs = query("""
            SELECT segment, count(*) AS clientes
            FROM main_marts.fct_risk_segments
            GROUP BY segment
            ORDER BY clientes DESC
        """)
        fig5 = px.pie(segs, values='clientes', names='segment',
                      title='Distribuição de Segmentos RFM',
                      color_discrete_sequence=px.colors.qualitative.Set3)
        st.plotly_chart(fig5, use_container_width=True)

    with col2:
        st.subheader("LTV por Segmento")
        ltv = query("""
            SELECT ltv_segment,
                count(*) AS clientes
            FROM main_marts.fct_customer_ltv
            GROUP BY ltv_segment
            ORDER BY clientes DESC
        """)
        fig6 = px.bar(ltv, x='ltv_segment', y='clientes',
                      text='clientes',
                      title='Clientes por Segmento de LTV',
                      color='clientes',
                      color_continuous_scale='Blues')
        st.plotly_chart(fig6, use_container_width=True)

    st.subheader("Qualidade por Canal de Aquisição")
    ltv_canal = query("""
        SELECT acquisition_channel,
            total_customers,
            round(approval_rate_pct, 1) AS aprovacao_pct,
            round(default_rate_pct, 2) AS default_pct,
            round(avg_ltv, 0) AS avg_ltv
        FROM main_intermediate.int_acquisition_quality
        ORDER BY default_pct ASC
    """)
    st.dataframe(ltv_canal, use_container_width=True)
    st.success("💡 **Insight:** Clientes com 2+ produtos têm LTV 4x maior e inadimplência 60% menor")

# ABA 4 — ML PREDICTIONS
with tab4:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("AUC-ROC", "0.999")
    col2.metric("F1-Score", "0.824")
    col3.metric("Recall", "97.3%")
    col4.metric("Accuracy", "99.5%")
    st.divider()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Top Features (SHAP)")
        features_static = pd.DataFrame({
            'feature': [
                'overall_default_rate', 'app_engagement_score',
                'products_count', 'total_contracts',
                'acquisition_channel_paid_search', 'avg_days_late',
                'income_declared', 'best_payment_streak',
                'days_since_last_login', 'age_group_senior'
            ],
            'importancia': [0.277, 0.119, 0.055, 0.043, 0.017, 0.015, 0.013, 0.011, 0.009, 0.007]
        })
        fig7 = px.bar(features_static, x='importancia', y='feature',
                      orientation='h',
                      title='Feature Importance (SHAP Values)',
                      color='importancia',
                      color_continuous_scale='Blues')
        fig7.update_layout(yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig7, use_container_width=True)

    with col2:
        st.subheader("Distribuição do Credit Score")
        scores = query("""
            SELECT credit_score, risk_tier
            FROM main_marts.fct_credit_score
            WHERE credit_score IS NOT NULL
        """)
        fig8 = px.histogram(scores, x='credit_score',
                            color='risk_tier',
                            nbins=50,
                            title='Distribuição do Credit Score',
                            color_discrete_map={
                                'very_low': '#22c55e',
                                'low': '#86efac',
                                'medium': '#fbbf24',
                                'high': '#f97316',
                                'very_high': '#ef4444'
                            })
        st.plotly_chart(fig8, use_container_width=True)

    st.subheader("Modelo de Predição — Como Funciona")
    st.markdown("""
    | Feature | Importância (SHAP) | Interpretação |
    |---|---|---|
    | overall_default_rate | 0.277 | Histórico de inadimplência |
    | app_engagement_score | 0.119 | Uso do app nos últimos 30 dias |
    | products_count | 0.055 | Diversificação de produtos |
    | avg_days_late | 0.048 | Atraso médio em pagamentos |
    | income_declared | 0.041 | Renda declarada |
    """)
    st.error("🤖 **Insight:** Queda de 50%+ no uso do app nos 30 dias anteriores prediz inadimplência com 73% de acurácia — sinal digital é o 2º preditor mais forte")
