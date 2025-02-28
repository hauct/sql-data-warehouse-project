import csv

def convert_dates():
    with open('datasets/source_crm/sales_details.csv', 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)  # Đọc toàn bộ dữ liệu vào bộ nhớ
        
    # Xử lý dữ liệu
    for row in rows:
        for col in ['sls_order_dt', 'sls_ship_dt', 'sls_due_dt']:
            date_str = row[col]
            if len(date_str) == 8 and date_str.isdigit():
                row[col] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    
    # Ghi đè file gốc
    with open('datasets/source_crm/sales_details.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    convert_dates()
    print("Đã cập nhật định dạng ngày thành công!")