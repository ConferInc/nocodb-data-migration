import os
import requests
import json

# --- Config ---
API_KEY = os.getenv("LITELLM_API_KEY")
BASE_URL = "https://litellm.confer.today"

def main():
    print("--- 🔍 STARTING CONNECTION TEST ---")
    
    if not API_KEY:
        print("❌ CRITICAL: LITELLM_API_KEY is missing!")
        return

    # We test the connection with a simple "Hello"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    
    # Simple payload. If this fails, the Model Name is wrong.
    payload = {
        "model": "gpt-3.5-turbo", 
        "messages": [{"role": "user", "content": "Hello"}]
    }

    print(f"📡 Connecting to: {BASE_URL}")
    
    try:
        response = requests.post(
            f"{BASE_URL}/v1/chat/completions", 
            json=payload, 
            headers=headers
        )
        
        # --- THIS PRINTS THE REAL ERROR ---
        print(f"🔢 Status Code: {response.status_code}")
        print(f"📜 Server Message: {response.text}") 

        if response.status_code == 200:
            print("✅ SUCCESS! The AI is working.")
        else:
            print("❌ FAILURE! Read the 'Server Message' above.")

    except Exception as e:
        print(f"💥 CRASH: {e}")

if __name__ == "__main__":
    main()
