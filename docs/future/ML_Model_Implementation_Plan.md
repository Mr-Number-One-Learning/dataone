# Future Plan: Stage 1 — Predictive Modeling

**Goal:** Extend the platform from descriptive ("what happened") to predictive ("what will happen") analytics using the curated gold layer as training data.  
**Effort:** 5–7 days  
**Prerequisite:** All previous stages complete. Requires `gold.daily_sales` to contain at least 90 days of correct, un-limited history (Stage 1 mandatory). For churn, requires `gold.customer_clv` (Stage 5) and `gold.product_sentiment` (Stage 4).

---

## Step 1.1 — Add MLflow to `docker-compose.yml`

```yaml
mlflow:
  image: ghcr.io/mlflow/mlflow:v2.14.0
  profiles: ["core"]
  container_name: dataone-mlflow
  networks: [dataone-net]
  ports:
    - "5000:5000"
  command: >
    mlflow server
    --backend-store-uri postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:${POSTGRES_PORT}/${POSTGRES_DB}
    --default-artifact-root /mlflow/artifacts
    --host 0.0.0.0
  volumes:
    - mlflow-artifacts:/mlflow/artifacts
  depends_on:
    postgres:
      condition: service_healthy
  mem_limit: "512m"
  cpus: "0.5"
```

Add `mlflow-artifacts` to the volumes section. Add `mlflow==2.14.0` to `requirements.txt`.

---

## Step 1.2 — Sales demand forecasting with Prophet

**New file:** `src/dataone/ml/sales_forecast.py`

```python
"""
Trains a Prophet time-series model on gold.daily_sales to produce a 30-day
demand forecast. Logs the model and metrics to MLflow.

Run: python -m dataone.ml.sales_forecast
"""
import mlflow
import pandas as pd
from prophet import Prophet

from dataone.utils.spark_session import build_spark_session
from dataone.utils.iceberg_helpers import table_identifier


def train_and_forecast(days_ahead: int = 30) -> pd.DataFrame:
    spark = build_spark_session("dataone-forecast")

    # Load gold.daily_sales into Pandas (small, aggregated table)
    df = (
        spark.read.format("iceberg")
        .load(table_identifier("gold", "daily_sales"))
        .select("sales_date", "total_revenue")
        .toPandas()
        .rename(columns={"sales_date": "ds", "total_revenue": "y"})
        .sort_values("ds")
    )

    with mlflow.start_run(run_name="sales_forecast"):
        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=True,
            daily_seasonality=False,
            changepoint_prior_scale=0.05,
        )
        model.fit(df)

        future = model.make_future_dataframe(periods=days_ahead)
        forecast = model.predict(future)

        # Log metrics and model
        mae = (forecast.set_index("ds")["yhat"] - df.set_index("ds")["y"]).abs().mean()
        mlflow.log_metric("mae", mae)
        mlflow.prophet.log_model(model, "prophet_model")

        return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].tail(days_ahead)


if __name__ == "__main__":
    forecast_df = train_and_forecast()
    print(forecast_df)
```

Add `prophet==1.1.5` to `requirements.txt`.

Write forecast output to a new `gold.sales_forecast` Iceberg table (schema:
`forecast_date DATE`, `predicted_revenue DOUBLE`, `lower_bound DOUBLE`, `upper_bound DOUBLE`,
`model_run_date DATE`).

Display in Grafana as a new panel on the Business KPIs dashboard: historical actual
revenue as a line, forecast as a shaded band.

---

## Step 1.3 — Customer churn prediction with XGBoost

**New file:** `src/dataone/ml/churn_model.py`

```python
"""
Binary classification: will this customer place another order in the next 90 days?
Features derived from gold/silver layer. Model tracked in MLflow.

Run: python -m dataone.ml.churn_model
"""
import mlflow
import mlflow.xgboost
import pandas as pd
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score

from dataone.utils.spark_session import build_spark_session
from dataone.utils.iceberg_helpers import table_identifier
from pyspark.sql import functions as F


def build_features(spark) -> pd.DataFrame:
    """
    Join gold and silver tables to build the churn feature set.
    Label: 1 = churned (no order in last 90 days), 0 = active.
    """
    clv = spark.read.format("iceberg").load(table_identifier("gold", "customer_clv"))
    sentiment = spark.read.format("iceberg").load(table_identifier("gold", "product_sentiment"))

    # Average sentiment of products this customer ordered
    fact_order_items = spark.read.format("iceberg").load(table_identifier("gold", "fact_order_items"))
    customer_sentiment = (
        fact_order_items.join(sentiment, on="product_id", how="left")
        .groupBy("customer_id")
        .agg(F.avg("avg_sentiment").alias("avg_product_sentiment"))
    )

    features = (
        clv.join(customer_sentiment, on="customer_id", how="left")
        .withColumn(
            "is_churned",
            F.when(F.col("days_since_last_order") > 90, 1).otherwise(0)
        )
        .select(
            "customer_id",
            "total_orders",
            "total_spend",
            "avg_order_value",
            "days_since_last_order",
            "avg_product_sentiment",
            F.col("segment").cast("string"),
            "is_churned",
        )
        .dropna()
        .toPandas()
    )
    features["segment_encoded"] = pd.Categorical(features["segment"]).codes
    return features.drop(columns=["segment"])


def train_churn_model():
    spark = build_spark_session("dataone-churn")
    features = build_features(spark)

    X = features.drop(columns=["customer_id", "is_churned"])
    y = features["is_churned"]

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    with mlflow.start_run(run_name="churn_model_v1"):
        model = XGBClassifier(n_estimators=100, max_depth=4, learning_rate=0.1,
                              use_label_encoder=False, eval_metric="logloss")
        model.fit(X_train, y_train)

        y_pred_proba = model.predict_proba(X_test)[:, 1]
        auc = roc_auc_score(y_test, y_pred_proba)
        mlflow.log_metric("roc_auc", auc)
        mlflow.xgboost.log_model(model, "churn_model")
        print(f"ROC AUC: {auc:.4f}")

    # Write scores back to the lakehouse
    scores = features[["customer_id"]].copy()
    scores["churn_probability"] = model.predict_proba(X)[:, 1]
    scores["risk_tier"] = pd.cut(
        scores["churn_probability"],
        bins=[0, 0.3, 0.6, 1.0],
        labels=["low", "medium", "high"]
    ).astype(str)
    return spark.createDataFrame(scores)
```

Add `xgboost==2.0.3 scikit-learn==1.5.0` to `requirements.txt`.

Write the churn scores to `gold.customer_churn_scores` (schema: `customer_id BIGINT`,
`churn_probability DOUBLE`, `risk_tier STRING`, `scored_at TIMESTAMP`).

Display in Grafana as a "Customer Retention" dashboard: a table of high-value customers
(`total_spend > 1000`) filtered to `risk_tier = 'high'`, so the marketing team can target
them with retention campaigns.

---

## Stage 1 Test Checklist

- [ ] MLflow UI at `http://localhost:5000` shows experiment runs with logged metrics and artifacts
- [ ] `SELECT * FROM dataone_catalog.gold.sales_forecast LIMIT 5` returns 30 rows with `forecast_date` in the future
- [ ] `SELECT risk_tier, COUNT(*) FROM dataone_catalog.gold.customer_churn_scores GROUP BY risk_tier` shows three tiers with non-zero counts
- [ ] Grafana "Inventory Forecast" panel shows a visible confidence band overlaid on historical data
- [ ] Grafana "Customer Retention" panel shows a table of high-risk, high-value customers
