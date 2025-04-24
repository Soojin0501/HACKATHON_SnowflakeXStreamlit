import streamlit as st, plotly.express as px
from snowflake.snowpark import Session
import pandas as pd

st.set_page_config(page_title="사용처별 탄소 배출", layout="wide")

# 1) 세션
sess = Session.builder.configs(st.secrets).create()

# 2) 월 선택 ▸ DISTINCT 가져오기
months = sess.sql("""
    SELECT DISTINCT STANDARD_YEAR_MONTH
    FROM CARD_CO2E_VW
    ORDER BY 1 DESC
""").to_pandas()["STANDARD_YEAR_MONTH"]

sel_month = st.sidebar.selectbox("조회 월", months)

# 3) 데이터 가져오기
# (위에서 Session, sel_month 얻은 뒤)

df = sess.sql(f"""
    SELECT  
        usage_clean          AS usage,
        SUM(sales_amt)       AS total_sales,
        SUM(CO2E_KG)         AS co2e_kg
    FROM CARD_CO2E_VW
    WHERE STANDARD_YEAR_MONTH = '{sel_month}'
    GROUP BY usage_clean
""").to_pandas()

df.columns = [c.lower() for c in df.columns]   # 컬럼 이름 통일하기

st.metric("총 배출량 (kg)", f"{df['co2e_kg'].sum():,.0f}")

fig = px.bar(df, x="usage", y="co2e_kg",
             title=f"{sel_month} 사용처별 CO₂ 배출",
             labels={"usage":"사용처", "co2e_kg":"kg CO₂e"})
st.plotly_chart(fig, use_container_width=True)


# 5) Raw table
with st.expander("원본 데이터 보기"):
    st.dataframe(df)
