import os
import requests
import json

# --- Configuration ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY")
LITELLM_BASE_URL = "https://litellm.confer.today"
REPO_NAME = os.getenv("GITHUB_REPOSITORY")
PR_NUMBER = os.getenv("PR_NUMBER")

gh_headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.diff",
}
ai_headers = {
    "Authorization": f"Bearer {LITELLM_API_KEY}",
    "Content-Type": "application/json"
}

def main():
    print(f"--- Starting Debug Review for PR #{PR_NUMBER} ---")

    # 1. Fetch Diff
    try:
        diff_url = f"https://api.github.com/repos/{REPO_NAME}/pulls/{PR_NUMBER}"
        diff_resp = requests.get(diff_url, headers=gh_headers)
        diff_resp.raise_for_status()
        diff_text = diff_resp.text
    except Exception as e:
        print(f"FATAL: Could not fetch diff. Error: {e}")
        return

    # 2. Ask LiteLLM
    print("Sending request to AI...")
    
    prompt = f"Review this code diff. Identify bugs. Be concise.\n\n{diff_text[:5000]}"

    # Trying a very standard model name first
    payload = {
        "model": "gpt-3.5-turbo", 
        "messages": [{"role": "user", "content": prompt}]
    }

    endpoint = f"{LITELLM_BASE_URL}/v1/chat/completions"
    
    response = requests.post(endpoint, json=payload, headers=ai_headers)

    # --- DEBUG SECTION ---
    if response.status_code != 200:
        print("❌ AI REQUEST FAILED!")
        print(f"Status Code: {response.status_code}")
        print(f"Response Body: {response.text}")  # <--- THIS IS WHAT WE NEED TO SEE
        return

    # 3. Success? Post Comment
    print("✅ AI Success! Posting comment...")
    review_body = response.json()['choices'][0]['message']['content']
    
    comment_url = f"https://api.github.com/repos/{REPO_NAME}/issues/{PR_NUMBER}/comments"
    requests.post(comment_url, headers=gh_headers, json={"body": f"## 🤖 AI Review\n\n{review_body}"})
    print("Comment posted.")

if __name__ == "__main__":
    main()
