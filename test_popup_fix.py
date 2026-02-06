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

def test_popup_fix():
    """Test that all popup fields are available and have content for modal display"""
    factory = RequestFactory()
    
    # Create a test request
    request = factory.get('/encar/api/combine/list', {'limit': '3', 'offset': '0', 'withTotal': '1'})
    
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
                print("\n" + "="*60)
                print("TESTING POPUP FUNCTIONALITY")
                print("="*60)
                
                # Test each row
                for i, row in enumerate(rows):
                    print(f"\n--- Row {i+1} ---")
                    
                    # Define the popup fields that should be clickable
                    popup_fields = {
                        '사고이력': 'accident',
                        '보험이력': 'insurance', 
                        '옵션': 'optStd',
                        '유상옵션': 'optPaid'
                    }
                    
                    for field_name, field_key in popup_fields.items():
                        if field_name in row:
                            value = row[field_name]
                            if value and value != "-" and str(value).strip():
                                # Simulate what would happen in the popup
                                display_value = str(value)
                                short_value = display_value[:50] + "..." if len(display_value) > 50 else display_value
                                
                                print(f"✅ {field_name}:")
                                print(f"   Short: {short_value}")
                                print(f"   Full (popup): {display_value[:100]}{'...' if len(display_value) > 100 else ''}")
                                print(f"   Length: {len(display_value)} chars")
                            else:
                                print(f"⚠️ {field_name}: Empty or no content ('{value}')")
                        else:
                            print(f"❌ {field_name}: Field not found in row")
                
                print("\n" + "="*60)
                print("SUMMARY")
                print("="*60)
                
                # Check if all required fields are present across all rows
                all_fields_present = True
                popup_fields = ['사고이력', '보험이력', '옵션', '유상옵션']
                
                for field in popup_fields:
                    field_found = all(field in row for row in rows)
                    has_content = any(row.get(field) and row.get(field) != "-" and str(row.get(field)).strip() for row in rows)
                    
                    if field_found and has_content:
                        print(f"✅ {field}: Present in all rows, has content in at least one row")
                    elif field_found:
                        print(f"⚠️ {field}: Present in all rows, but no meaningful content found")
                    else:
                        print(f"❌ {field}: Missing from some rows")
                        all_fields_present = False
                
                if all_fields_present:
                    print(f"\n🎉 All popup fields are properly configured!")
                    print(f"   - Standard options (옵션) column has been restored")
                    print(f"   - All clickable fields should show popups with full content")
                else:
                    print(f"\n⚠️ Some issues found with popup field configuration")
                    
            else:
                print("No rows returned from API")
        else:
            print(f"API Error: {data.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_popup_fix()