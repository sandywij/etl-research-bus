#!/usr/bin/env python3
"""
Test API connectivity before deploying pipeline
Usage: python test_api.py
"""

import requests
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("Note: python-dotenv not installed. Make sure environment variables are set.")

def test_api():
    api_url = os.getenv('API_URL')
    api_token = os.getenv('API_TOKEN')
    token_header = os.getenv('API_TOKEN_HEADER', 'Authorization')
    
    # Validate inputs
    if not api_url:
        print("❌ ERROR: API_URL not set")
        print("   Set with: export API_URL='https://your-api.com/data'")
        return False
    
    if not api_token:
        print("❌ ERROR: API_TOKEN not set")
        print("   Set with: export API_TOKEN='your-token'")
        return False
    
    # Build headers
    headers = {
        token_header: api_token,
        'User-Agent': 'Test-Pipeline/1.0'
    }
    
    print(f"\n📋 Testing API Connectivity")
    print(f"{'='*50}")
    print(f"API URL: {api_url}")
    print(f"Token header: {token_header}")
    print(f"Token: {api_token[:20]}...{api_token[-5:]}" if len(api_token) > 25 else f"Token: {api_token}")
    print(f"{'='*50}\n")
    
    try:
        print("⏳ Sending request...")
        response = requests.get(
            api_url,
            params={'location': 'TestLocation'},
            headers=headers,
            timeout=15
        )
        
        print(f"✓ Response received!")
        print(f"  Status: {response.status_code}")
        print(f"  Content-Type: {response.headers.get('content-type', 'unknown')}")
        print(f"  Response size: {len(response.content)} bytes\n")
        
        # Try to parse JSON
        try:
            data = response.json()
            print(f"✓ Valid JSON response:")
            print(f"  {str(data)[:200]}...\n")
        except:
            print(f"⚠ Response is not JSON:")
            print(f"  {response.text[:200]}\n")
        
        # Check status code
        if response.status_code == 200:
            print("✅ API test PASSED")
            print("   Your API is accessible and returning data")
            return True
        
        elif response.status_code == 401:
            print("❌ API test FAILED: 401 Unauthorized")
            print("   Check your API_TOKEN")
            return False
        
        elif response.status_code == 403:
            print("❌ API test FAILED: 403 Forbidden")
            print("   Token is valid but lacks permissions")
            return False
        
        elif response.status_code == 404:
            print("❌ API test FAILED: 404 Not Found")
            print("   Check your API_URL")
            return False
        
        else:
            print(f"⚠ API returned status {response.status_code}")
            print(f"   This may or may not be an error depending on your API")
            return response.status_code < 500
    
    except requests.exceptions.Timeout:
        print("❌ API test FAILED: Request timeout")
        print("   Check if API is reachable and responding")
        return False
    
    except requests.exceptions.ConnectionError as e:
        print("❌ API test FAILED: Connection error")
        print(f"   {e}")
        print("   Check if API URL is correct and accessible")
        return False
    
    except Exception as e:
        print(f"❌ API test FAILED: {e}")
        return False

if __name__ == "__main__":
    success = test_api()
    sys.exit(0 if success else 1)
