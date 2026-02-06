import os
import sys
import django
from django.conf import settings

# Add the project directory to Python path
sys.path.append('/Users/aiden/work/encar_data')

# Set up Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'encar_admin.settings')
django.setup()

from django.test import RequestFactory
from encar.views import combine_list_api

def test_list_api_fields():
    """Test that the list API returns the expected field names for accident history, insurance history, and paid options"""
    factory = RequestFactory()
    
    # Create a test request with a small limit to get sample data
    request = factory.get('/encar/api/combine/list', {'limit': '5', 'offset': '0', 'withTotal': '1'})
    
    try:
        # Call the API function
        response = combine_list_api(request)
        
        print("✅ API endpoint called successfully")
        print(f"Status code: {response.status_code}")
        
        # Parse the JSON response
        import json
        content = response.content.decode('utf-8')
        data = json.loads(content)
        
        print(f"Response OK: {data.get('ok', False)}")
        
        if data.get('ok'):
            rows = data.get('rows', [])
            print(f"Number of rows returned: {len(rows)}")
            
            if rows:
                # Check the first row for the expected fields
                first_row = rows[0]
                print("\nChecking field names in first row:")
                
                # Check for the three fields we're interested in
                target_fields = ['사고이력', '보험이력', '유상옵션']
                
                for field in target_fields:
                    if field in first_row:
                        value = first_row[field]
                        print(f"✅ {field}: '{value}' (type: {type(value).__name__})")
                    else:
                        print(f"❌ {field}: NOT FOUND")
                
                print(f"\nAll available fields in first row:")
                for key, value in first_row.items():
                    if isinstance(value, str) and len(value) > 50:
                        display_value = value[:50] + "..."
                    else:
                        display_value = value
                    print(f"  {key}: {display_value}")
                    
            else:
                print("No rows returned from API")
        else:
            print(f"API Error: {data.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_list_api_fields()