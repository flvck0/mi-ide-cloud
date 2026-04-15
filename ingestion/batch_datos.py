

import pandas as pd
import requests


def leer_datos_batch(subject):
    url = f"https://openlibrary.org/subjects/{subject}.json?limit=50"
    response = requests.get(url)
    data = response.json()
    df = pd.json_normalize(data['works'])
    df = df[['title', 'key', 'first_publish_year']]
    print(f"Batch {len(df)} book records.")
    return df