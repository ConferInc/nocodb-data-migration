import os
import requests
import json

# --- Config ---
API_KEY = os.getenv("LITELLM_API_KEY")
BASE_URL = "https://litellm.confer.today"

def main():
    print("--- 🔍 STARTING CONNECTION TEST ---")
    
    # 1. Check if Key exists
    if not API_KEY:
        print("❌ CRITICAL: LITELLM_API_KEY is missing!")
        return
    print(f"✅ API Key found (Length: {len(API_KEY)})")

    # 2. Test Connection (Hello World)
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    # We try the most standard request possible
    payload = {
        "model": "gpt-3.5-turbo", 
        "messages": [{"role": "user", "content": "Say 'Connection Successful' if you can hear me."}]
    }

    print(f"📡 Sending request to: {BASE_URL}/v1/chat/completions")
    
    try:
        response = requests.post(
            f"{BASE_URL}/v1/chat/completions", 
            json=payload, 
            headers=headers
        )
        
        # --- THE MOMENT OF TRUTH ---
        print(f"🔢 Status Code: {response.status_code}")
        print(f"📜 Server Response: {response.text}")  # <--- THIS WILL TELL US THE ERROR

        if response.status_code == 200:
            print("✅ SUCCESS! The AI is talking to us.")
        else:
            print("❌ FAILURE! The server rejected us.")

    except Exception as e:
        print(f"💥 CRASH: {e}")

if __name__ == "__main__":
    main()
