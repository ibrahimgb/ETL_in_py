import csv

def save_as_csv(processed_rows, output_file):
    if not processed_rows:
        print("No data to save")
        return

    keys = processed_rows[0].keys()   #get headers
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(processed_rows)

    print(f"Data saved to {output_file}")
