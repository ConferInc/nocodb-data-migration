import os
import requests
import json

# --- Configuration ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY")
LITELLM_BASE_URL = "https://litellm.confer.today"
REPO_NAME = os.getenv("GITHUB_REPOSITORY")
PR_NUMBER = os.getenv("PR_NUMBER")

# --- Standard Headers ---
gh_headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.diff",
}
ai_headers = {
    "Authorization": f"Bearer {LITELLM_API_KEY}",
    "Content-Type": "application/json"
}

def main():
    print(f"--- 🤖 Starting AI Review for PR #{PR_NUMBER} ---")

    # 1. Fetch the Code Changes (Diff)
    try:
        diff_url = f"https://api.github.com/repos/{REPO_NAME}/pulls/{PR_NUMBER}"
        diff_resp = requests.get(diff_url, headers=gh_headers)
        diff_resp.raise_for_status()
        diff_text = diff_resp.text
    except Exception as e:
        print(f"❌ Error fetching diff: {e}")
        return

    if len(diff_text) < 10:
        print("⚠️ Diff is empty. Nothing to review.")
        return

    # 2. Ask the AI (Using the proven model: gpt-3.5-turbo)
    print("Sending code to AI...")
    
    # We limit the diff to 6000 chars to prevent timeouts
    prompt = f"""
    You are a Senior Code Reviewer. Review this GitHub Pull Request Diff.
    
    Instructions:
    - Look for bugs, security leaks, and logic errors.
    - Be concise and professional.
    - Use Markdown formatting.
    
    DIFF:
    {diff_text[:6000]}
    """

    payload = {
        "model": "gpt-3.5-turbo",
        "messages": [{"role": "user", "content": prompt}]
    }

    try:
        response = requests.post(
            f"{LITELLM_BASE_URL}/v1/chat/completions",
            json=payload,
            headers=ai_headers
        )
        
        if response.status_code != 200:
            print(f"❌ AI Error {response.status_code}: {response.text}")
            return
            
        ai_content = response.json()['choices'][0]['message']['content']
        print("✅ AI Review Generated!")

    except Exception as e:
        print(f"❌ Connection Failed: {e}")
        return

    # 3. Post the Comment to GitHub
    try:
        comment_url = f"https://api.github.com/repos/{REPO_NAME}/issues/{PR_NUMBER}/comments"
        # Switch header for posting comments
        post_headers = gh_headers.copy()
        post_headers["Accept"] = "application/vnd.github+json"
        
        payload = {"body": f"## 🤖 AI Code Review\n\n{ai_content}"}
        
        requests.post(comment_url, headers=post_headers, json=payload)
        print("🚀 Comment posted to PR successfully.")
        
    except Exception as e:
        print(f"❌ Error posting comment: {e}")

if __name__ == "__main__":
    main()
