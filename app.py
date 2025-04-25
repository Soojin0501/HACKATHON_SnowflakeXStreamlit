import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, sum as sf_sum

# --- 1. 세션 연결 ---
session = Session.builder.configs(st.secrets["snowflake"]).create()

# --- 2. 제목 ---
st.title("📊 탄소 배출량 대시보드")

# --- 3. 데이터 로딩 ---
st.subheader("카드 소비 기반 탄소 배출 데이터")
df = session.table("CARD_CO2E_VW")

# --- 4. 필터 UI ---
years = df.select(col("YEAR")).distinct().sort(col("YEAR")).to_pandas()["YEAR"].tolist()
selected_year = st.selectbox("연도 선택", years)

months = df.filter(col("YEAR") == selected_year)\
           .select(col("MONTH")).distinct().sort(col("MONTH"))\
           .to_pandas()["MONTH"].tolist()
selected_month = st.selectbox("월 선택", months)

city_options = df.select(col("CITY_NAME")).distinct().sort(col("CITY_NAME")).to_pandas()["CITY_NAME"].tolist()
selected_city = st.selectbox("도시 선택", city_options)

# 성별, 연령대, 라이프스타일 추가 필터
gender_options = df.select(col("GENDER")).distinct().sort(col("GENDER")).to_pandas()["GENDER"].tolist()
selected_gender = st.selectbox("성별 선택", gender_options)

age_options = df.select(col("AGE_GROUP")).distinct().sort(col("AGE_GROUP")).to_pandas()["AGE_GROUP"].tolist()
selected_age = st.selectbox("연령대 선택", age_options)

lifestyle_options = df.select(col("LIFESTYLE_KOR")).distinct().sort(col("LIFESTYLE_KOR")).to_pandas()["LIFESTYLE_KOR"].tolist()
selected_lifestyle = st.selectbox("라이프스타일 선택", lifestyle_options)

# --- 5. 필터링 (연도, 월, 도시, 성별, 연령대, 라이프스타일) ---
filtered_df = df.filter(
    (col("YEAR") == selected_year) & 
    (col("MONTH") == selected_month) & 
    (col("CITY_NAME") == selected_city) &
    (col("GENDER") == selected_gender) &
    (col("AGE_GROUP") == selected_age) &
    (col("LIFESTYLE_KOR") == selected_lifestyle)
)

# --- 6. 총 배출량 요약 ---
st.markdown("### ✅ 총 탄소 배출량 (kg)")
total_emission = (
    filtered_df.agg(sf_sum(col("CO2E_KG")).alias("총_탄소배출량(kg)"))
    .to_pandas()
)
st.dataframe(total_emission)

# --- 7. 월별 탄소 배출 추이 (선택된 조건 기반) ---
st.markdown("### 📈 월별 탄소 배출 추이")
trend_df = df.filter(
    (col("YEAR") == selected_year) & 
    (col("CITY_NAME") == selected_city) &
    (col("GENDER") == selected_gender) &
    (col("AGE_GROUP") == selected_age) &
    (col("LIFESTYLE_KOR") == selected_lifestyle)
)
monthly_emission = (
    trend_df.group_by("MONTH")
    .agg(sf_sum(col("CO2E_KG")).alias("월_탄소배출량(kg)"))
    .sort(col("MONTH"))
    .to_pandas()
)
st.line_chart(monthly_emission.set_index("MONTH"))

# --- 8. 라이프스타일별 탄소 배출 추이 ---
st.markdown("### 🧬 라이프스타일별 탄소 배출 추이")
lifestyle_monthly = (
    trend_df.group_by("MONTH", "LIFESTYLE_KOR")
    .agg(sf_sum(col("CO2E_KG")).alias("배출량(kg)"))
    .sort(col("MONTH"))
    .to_pandas()
)
lifestyle_pivot = lifestyle_monthly.pivot(index="MONTH", columns="LIFESTYLE_KOR", values="배출량(kg)")
st.line_chart(lifestyle_pivot)

# --- 8-1. 성별 탄소 배출 추이 ---
st.markdown("### 🚻 성별 탄소 배출 추이")
gender_monthly = (
    trend_df.group_by("MONTH", "GENDER")
    .agg(sf_sum(col("CO2E_KG")).alias("배출량(kg)"))
    .sort(col("MONTH"))
    .to_pandas()
)
gender_pivot = gender_monthly.pivot(index="MONTH", columns="GENDER", values="배출량(kg)")
st.line_chart(gender_pivot)

# --- 8-2. 연령대별 탄소 배출 추이 ---
st.markdown("### 🎂 연령대별 탄소 배출 추이")
age_monthly = (
    trend_df.group_by("MONTH", "AGE_GROUP")
    .agg(sf_sum(col("CO2E_KG")).alias("배출량(kg)"))
    .sort(col("MONTH"))
    .to_pandas()
)
age_pivot = age_monthly.pivot(index="MONTH", columns="AGE_GROUP", values="배출량(kg)")
st.line_chart(age_pivot)

# --- 9. 🥇 사용처별 Top 3 ---
st.markdown("### 🥇 사용처별 Top 3 탄소 배출")

top_usage_df = (
    filtered_df.group_by("USAGE_CLEAN")
    .agg(sf_sum(col("CO2E_KG")).alias("배출량(kg)"))
    .sort(col("배출량(kg)").desc())
    .limit(3)
    .to_pandas()
)

st.dataframe(top_usage_df)

# --- 10. 원형 차트 시각화 ---
fig, ax = plt.subplots()
ax.pie(top_usage_df["배출량(kg)"], labels=top_usage_df["USAGE_CLEAN"], autopct="%1.1f%%", startangle=140)
ax.axis("equal")
st.pyplot(fig)

# --- 11. 💰 탄소 절감 기반 포인트 리워드 ---
st.markdown("### 💰 탄소 절감 포인트 리워드")

# 현재 달 배출량
this_month_emission = total_emission.iloc[0, 0]

# 전월 데이터 로딩
prev_month = str(int(selected_month) - 1).zfill(2)
prev_df = df.filter(
    (col("YEAR") == selected_year) &
    (col("MONTH") == prev_month) &
    (col("CITY_NAME") == selected_city) &
    (col("GENDER") == selected_gender) &
    (col("AGE_GROUP") == selected_age) &
    (col("LIFESTYLE_KOR") == selected_lifestyle)
)

prev_emission_df = (
    prev_df.agg(sf_sum(col("CO2E_KG")).alias("지난달_배출량(kg)"))
    .to_pandas()
)

if not prev_emission_df.empty:
    prev_emission = prev_emission_df.iloc[0, 0]
    reduction = prev_emission - this_month_emission
    if reduction > 0:
        points = round(reduction * 10)  # 1kg 감축당 10포인트
        st.success(f"🎉 지난달보다 {reduction:,.0f}kg CO₂를 절감하여 {points:,}포인트를 획득했습니다!")
    else:
        st.warning("❗ 이번 달은 탄소 배출이 증가했어요. 다음 달엔 절감해봐요!")
else:
    st.info("ℹ️ 전월 데이터가 없어서 비교할 수 없습니다.")

