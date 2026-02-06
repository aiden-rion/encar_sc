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
from encar.views import combine_price_analysis_api

def test_price_analysis_api():
    """Test the new price analysis API endpoint"""
    factory = RequestFactory()
    
    # Create a test request
    request = factory.get('/encar/api/combine/price-analysis', {'keyword': '', 'sample': '100'})
    
    try:
        # Call the API function
        response = combine_price_analysis_api(request)
        
        print("✅ API endpoint called successfully")
        print(f"Status code: {response.status_code}")
        
        # Parse the JSON response
        import json
        content = response.content.decode('utf-8')
        data = json.loads(content)
        
        print(f"Response OK: {data.get('ok', False)}")
        
        if data.get('ok'):
            analysis = data.get('analysis', [])
            mileage_ranges = data.get('mileage_ranges', [])
            
            print(f"Number of years analyzed: {len(analysis)}")
            print(f"Mileage ranges: {mileage_ranges}")
            
            if analysis:
                print("\nSample data (first year):")
                first_year = analysis[0]
                print(f"Year: {first_year.get('year')}")
                for range_data in first_year.get('mileage_ranges', []):
                    if range_data.get('count', 0) > 0:
                        print(f"  {range_data.get('range')}: {range_data.get('avg_price'):,}원 (매물 {range_data.get('count')}개)")
        else:
            print(f"API Error: {data.get('error', 'Unknown error')}")
            
    except Exception as e:
        print(f"❌ Error testing API: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_price_analysis_api()