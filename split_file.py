import csv
from math import ceil

INPUT_FILE = 'locations.csv'
OUTPUT_1   = 'locations_1.csv'
OUTPUT_2   = 'locations_2.csv'

with open(INPUT_FILE) as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    rows = list(reader)

mid = ceil(len(rows) / 2)
split1 = rows[:mid]
split2 = rows[mid:]

for path, subset in ((OUTPUT_1, split1), (OUTPUT_2, split2)):
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(subset)

print(f"Total locations: {len(rows)}")
print(f"{OUTPUT_1}: {len(split1)} locations")
print(f"{OUTPUT_2}: {len(split2)} locations")