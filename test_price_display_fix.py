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

def test_price_display_logic():
    """Test the price display logic with sample data"""
    
    # Test the JavaScript logic in Python to verify it works
    def format_price_display(avg_price):
        if avg_price >= 10000:
            display_price = round(avg_price / 10000)
            unit = "만원"
        else:
            display_price = round(avg_price / 1000)
            unit = "천원"
        return f"{display_price}{unit}"
    
    # Test cases
    test_cases = [
        (6270, "6천원"),      # Small price from our previous test
        (15000, "2만원"),     # Medium price
        (50000, "5만원"),     # Large price
        (1500, "2천원"),      # Very small price
        (9999, "10천원"),     # Edge case just under 10000
        (10000, "1만원"),     # Edge case exactly 10000
    ]
    
    print("Testing price display logic:")
    for price, expected in test_cases:
        result = format_price_display(price)
        status = "✅" if result == expected else "❌"
        print(f"{status} {price}원 -> {result} (expected: {expected})")

def test_api_with_display():
    """Test the API and show how prices would be displayed"""
    factory = RequestFactory()
    
    # Create a test request
    request = factory.get('/encar/api/combine/price-analysis', {'keyword': '', 'sample': '100'})
    
    try:
        # Call the API function
        response = combine_price_analysis_api(request)
        
        if response.status_code == 200:
            import json
            content = response.content.decode('utf-8')
            data = json.loads(content)
            
            if data.get('ok') and data.get('analysis'):
                print("\nAPI Response - Sample price displays:")
                
                for year_data in data['analysis'][:3]:  # Show first 3 years
                    print(f"\nYear {year_data['year']}:")
                    for range_data in year_data['mileage_ranges']:
                        if range_data['count'] > 0:
                            avg_price = range_data['avg_price']
                            
                            # Apply the same logic as frontend
                            if avg_price >= 10000:
                                display_price = round(avg_price / 10000)
                                unit = "만원"
                            else:
                                display_price = round(avg_price / 1000)
                                unit = "천원"
                            
                            print(f"  {range_data['range']}: {display_price}{unit} (원래: {avg_price:,}원, 매물 {range_data['count']}개)")
            else:
                print("No analysis data available")
        else:
            print(f"API call failed with status {response.status_code}")
            
    except Exception as e:
        print(f"Error testing API: {e}")

if __name__ == "__main__":
    test_price_display_logic()
    test_api_with_display()