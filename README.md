# COVID-19 Impact on Air Quality Dashboard

📊 This Power BI dashboard was created to highlight the effect of COVID-19 and the subsequent slowdown of human activities on air pollutant concentrations in various European countries.

## 📡 Data Source
The data comes from the open-source air quality platform [OpenAQ](https://openaq.org/) ([API](https://api.openaq.org/)). The data has been extracted, transformed, and loaded through the REST API using a Python script. The script can be found in the project GitHub repository: [13_Covid_Effect_On_Air_Quality](https://github.com/slvg01/13_Covid_Effect_On_Air_Quality).

## 🔎 Key Pollutants
The pollution markers CO, NO₂, and PM10 represent different air pollutants that affect both human health and the environment. Below is a brief explanation of each:

### 1. CO (Carbon Monoxide) 🏭
- **Source:** Mainly produced by incomplete combustion of fuels (vehicles, heating, industries).
- **Health Effects:** At high concentrations, CO reduces the blood’s ability to carry oxygen, leading to headaches, dizziness, and in severe cases, poisoning.
- **Critical Threshold:** The WHO recommends not exceeding **10 mg/m³ over 8 hours**.

### 2. NO₂ (Nitrogen Dioxide) 🚗
- **Source:** Emitted from diesel engines, power plants, and industrial processes.
- **Health Effects:** A respiratory irritant that worsens asthma and increases the risk of lung infections.
- **Critical Threshold:** The WHO recommends not exceeding **25 µg/m³ as an annual average** and **200 µg/m³ for hourly peaks**.

### 3. PM10 (Particulate Matter ≤10 µm) 🌫️
- **Source:** Comes from combustion (vehicles, wood heating), industrial dust, wildfires, and soil erosion.
- **Health Effects:** Can enter the respiratory system, contributing to cardiovascular and respiratory diseases.
- **Critical Threshold:** The WHO recommends not exceeding **15 µg/m³ as an annual average** and **45 µg/m³ as a daily average**.

---
## 📂 You Will Find:
✅ The **Python code** to get and clean the data through API
✅ The **.pbix Power BI file**
✅ A **PDF export** of the Power BI dashboard
✅ A **short video** showing the map animation related to pollution evolution over time
✅ The  **requirement.txt** file that gives the necessary library to install ti run teh script

📌 **Repository:** [GitHub - 13_Covid_Effect_On_Air_Quality](https://github.com/slvg01/13_Covid_Effect_On_Air_Quality) 🚀