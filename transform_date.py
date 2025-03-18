import csv
from datetime import datetime

def convert_date_str(date_str, format_type):
    """Chuyển đổi chuỗi ngày tháng theo các định dạng khác nhau"""
    try:
        if format_type == 'yyyymmdd':
            if len(date_str) == 8 and date_str.isdigit():
                return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        
        elif format_type == 'ddmmyyyy':
            parts = date_str.split('/')
            if len(parts) == 3 and all(p.isdigit() for p in parts):
                return f"{parts[2]}-{parts[1]}-{parts[0]}"
        
        return None
    except:
        return None

def process_file(filename, date_columns):
    """Xử lý file CSV với các cột ngày tháng cần chuyển đổi"""
    with open(filename, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        
    for row in rows:
        for col, fmt in date_columns.items():
            date_str = str(row[col]).strip()
            
            # Xử lý các trường hợp đặc biệt
            if not date_str or date_str.lower() in ['null', 'nan', 'na', '']:
                row[col] = None
                continue
                
            # Chuyển đổi theo định dạng
            converted = convert_date_str(date_str, fmt)
            row[col] = converted if converted else None
    
    # Ghi đè file gốc
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=reader.fieldnames)
        writer.writeheader()
        writer.writerows(rows)

def convert_dates():
    # Xử lý sales_details.csv
    process_file(
        'datasets/source_crm/sales_details.csv',
        {
            'sls_order_dt': 'yyyymmdd',
            'sls_ship_dt': 'yyyymmdd', 
            'sls_due_dt': 'yyyymmdd'
        }
    )
    
    # Xử lý cust_info.csv
    process_file(
        'datasets/source_crm/cust_info.csv',
        {
            'cst_create_date': 'ddmmyyyy'
        }
    )

if __name__ == "__main__":
    convert_dates()
    print("Đã cập nhật định dạng ngày thành công cho tất cả files!")