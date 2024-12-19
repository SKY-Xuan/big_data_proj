import pandas as pd
import re

def extract_brand(car_name):
    # Use regex to extract the brand (first word) from the car name
    match = re.match(r'^([\u4e00-\u9fff]+|\w+)', car_name)
    if match:
        return match.group(1)
    return ''

# Read the CSV file
df = pd.read_csv('guazi.csv', encoding='utf-8')

# Create a new column 'brand' by extracting the brand from 'leixing'
df['brand'] = df['leixing'].apply(extract_brand)

# Save the updated DataFrame to a new CSV file
df.to_csv('guolv.csv', index=False, encoding='utf-8')

print("Brand extraction complete. New file created.")