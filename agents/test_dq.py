import sys
from pathlib import Path
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)

sys.path.append(str(Path(__file__).parent))
from etl_agent import ETLAgent

def test_dq():
    agent = ETLAgent()
    
    # Tạo dummy data để test
    df = pd.DataFrame({
        'date_key': [20260601, 20260602, 20260603],
        'room_type_id': [1, 1, 2],
        'channel_id': [1, 1, 1],
        'segment_id': [1, 1, 1],
        'promotion_id': [1, 1, 1],
        'rooms_sold': [10, 0, 5],                 # Dòng 2 có rooms_sold = 0
        'total_rooms_available': [10, 10, 4],     # Dòng 3 có rooms_sold > available
        'gross_room_revenue': [1000000, 500000, -200000], # Dòng 2 sai logic, Dòng 3 bị số âm
        'listed_room_revenue': [1200000, 0, 0],
        'net_room_revenue': [900000, 0, 0]
    })
    
    print("DataFrame gốc:")
    print(df)
    
    # Áp dụng rule
    df_clean, errors = agent.apply_business_rules('fact_room_revenue_daily', df)
    
    print("\nLỗi phát hiện được:")
    for e in errors:
        print(" -", e)
        
    print("\nDataFrame sau khi lọc rác:")
    print(df_clean)

if __name__ == "__main__":
    test_dq()
