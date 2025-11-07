import pandas as pd
url = "https://aviationweather.gov/api/data/metar?ids=KDFW&hours=72&format=json"
df = pd.read_csv(url)
print(df.head())