import requests
from datetime import datetime, timedelta, timezone

# Replace with your actual OpenAQ API key
API_KEY = "01946b8515c545443cdcd262a884a88dab1be54962aad37f4f93c3420cc49844"
headers = {"X-API-Key": API_KEY}


# Set your parameters
city = "Brussels"
parameter = "pm25"

# Define date range: past 10 years
date_to = datetime.now(timezone.utc)
date_from = date_to - timedelta(days=365 * 10)

# Format dates as ISO8601 strings
date_to_str = date_to.strftime("%Y-%m-%dT%H:%M:%SZ")
date_from_str = date_from.strftime("%Y-%m-%dT%H:%M:%SZ")

# Use the aggregations endpoint with monthly aggregation
url = "https://api.openaq.org/v3/measurements?city=Amsterdam&parameter=so2"

# Query parameters:
# - city: filter for Brussels
# - parameter: filter for pm25 measurements
# - period: 'month' requests monthly aggregated data
# - date_from and date_to: time window
# - limit and page: for pagination
params = {
    'city': city,
    'parameter': parameter,
    'period': 'month',
    'date_from': date_from_str,
    'date_to': date_to_str,
    'limit': 100,
    'page': 1
}

all_results = []

while True:
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Error:", response.status_code, response.text)
        break

    data = response.json()
    results = data.get('results', [])

    # If no more data, exit loop
    if not results:
        break

    all_results.extend(results)
    print(f"Retrieved {len(results)} records on page {params['page']}.")

    # If returned fewer than limit, we're on the last page
    if len(results) < params['limit']:
        break

    # Otherwise, fetch the next page
    params['page'] += 1

print(f"Total monthly aggregation records retrieved: {len(all_results)}")