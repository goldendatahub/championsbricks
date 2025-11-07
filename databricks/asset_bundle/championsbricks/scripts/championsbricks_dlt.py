import dlt
import pandas as pd
import requests
import io

@dlt.table(
    name="bronze_github_prices",
    comment="Raw prices loaded from GitHub via pandas + requests"
)
def bronze_github_prices():
    
    url = "https://raw.githubusercontent.com/goldendatahub/championsbricks/refs/heads/main/data/sample/football_teams.csv"

    # ✅ Download file using requests
    response = requests.get(url)
    response.raise_for_status()

    # ✅ Read it into a Pandas DataFrame
    pdf = pd.read_csv(io.StringIO(response.text), sep=";")

    # ✅ Convert Pandas → Spark DataFrame
    df = spark.createDataFrame(pdf)

    return df
