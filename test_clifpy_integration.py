#!/usr/bin/env python3
"""
Integration test for clifpy integration in CLIF-Lighthouse
"""
import sys
import os
import tempfile
import io

# Add the app directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from common_qc import read_data

# Test data for different table types
test_data = {
    'adt': '''patient_id,hospitalization_id,in_dttm,out_dttm,location_name
1,101,"2023-01-01 10:00:00","2023-01-01 12:00:00",ICU-A
2,102,"2023-01-01 11:00:00","2023-01-01 13:00:00",ICU-B''',
    
    'labs': '''patient_id,hospitalization_id,lab_order_dttm,lab_collect_dttm,lab_result_dttm,lab_name,lab_category,lab_value
1,101,"2023-01-01 08:00:00","2023-01-01 08:15:00","2023-01-01 10:00:00",Hemoglobin,Hematology,12.5
2,102,"2023-01-01 09:00:00","2023-01-01 09:15:00","2023-01-01 11:00:00",Glucose,Chemistry,95''',
    
    'patient': '''patient_id,date_of_birth,sex
1,"1990-01-01",M
2,"1985-05-15",F'''
}

class MockStreamlitFile:
    """Mock Streamlit UploadedFile for testing"""
    def __init__(self, name, content):
        self.name = name
        self._content = content.encode('utf-8')
    
    def getvalue(self):
        return self._content
    
    def read(self, size=-1):
        return self._content
    
    def seek(self, pos):
        pass

def test_table_loading():
    """Test loading different table types"""
    results = []
    
    for table_type, data in test_data.items():
        print(f"\nTesting {table_type} table...")
        
        # Test with clifpy table type
        mock_file = MockStreamlitFile(f'clif_{table_type}.csv', data)
        try:
            df_clifpy = read_data(mock_file, table_type=f'clif_{table_type}')
            results.append((f'clif_{table_type}', 'SUCCESS', df_clifpy.shape, list(df_clifpy.columns)))
            print(f"  ✓ clifpy loading: {df_clifpy.shape} - {list(df_clifpy.columns)}")
        except Exception as e:
            results.append((f'clif_{table_type}', 'FAILED', str(e), None))
            print(f"  ✗ clifpy loading failed: {e}")
        
        # Test pandas fallback
        mock_file = MockStreamlitFile(f'{table_type}.csv', data)
        try:
            df_pandas = read_data(mock_file)
            results.append((f'{table_type}_pandas', 'SUCCESS', df_pandas.shape, list(df_pandas.columns)))
            print(f"  ✓ pandas fallback: {df_pandas.shape} - {list(df_pandas.columns)}")
        except Exception as e:
            results.append((f'{table_type}_pandas', 'FAILED', str(e), None))
            print(f"  ✗ pandas fallback failed: {e}")
    
    # Test unsupported table type
    print(f"\nTesting unsupported table type...")
    mock_file = MockStreamlitFile('clif_microbiology_culture.csv', test_data['labs'])
    try:
        df_unsupported = read_data(mock_file, table_type='clif_microbiology_culture')
        results.append(('unsupported', 'SUCCESS', df_unsupported.shape, list(df_unsupported.columns)))
        print(f"  ✓ unsupported table fallback: {df_unsupported.shape}")
    except Exception as e:
        results.append(('unsupported', 'FAILED', str(e), None))
        print(f"  ✗ unsupported table fallback failed: {e}")
    
    return results

def main():
    print("CLIF-Lighthouse clifpy Integration Test")
    print("=" * 50)
    
    results = test_table_loading()
    
    print("\n" + "=" * 50)
    print("TEST SUMMARY:")
    
    success_count = sum(1 for result in results if result[1] == 'SUCCESS')
    total_count = len(results)
    
    for test_name, status, details, columns in results:
        status_icon = "✓" if status == "SUCCESS" else "✗"
        print(f"  {status_icon} {test_name}: {status}")
        if status == "SUCCESS" and isinstance(details, tuple):
            print(f"    Shape: {details}, Columns: {len(columns) if columns else 0}")
    
    print(f"\nOverall: {success_count}/{total_count} tests passed")
    
    if success_count == total_count:
        print("🎉 All tests passed! clifpy integration is working correctly.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    exit(main())