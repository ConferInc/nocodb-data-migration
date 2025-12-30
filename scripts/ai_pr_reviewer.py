import os
import requests
import json

# --- Configuration ---
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
LITELLM_API_KEY = os.getenv("LITELLM_API_KEY")
LITELLM_BASE_URL = "https://litellm.confer.today"
REPO_NAME = os.getenv("GITHUB_REPOSITORY")
PR_NUMBER = os.getenv("PR_NUMBER")

# --- Headers ---
gh_headers = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3.diff",
}
ai_headers = {
    "Authorization": f"Bearer {LITELLM_API_KEY}",
    "Content-Type": "application/json"
}

def main():
    if not PR_NUMBER:
        print("Skipping: Not a PR event.")
        return

    print(f"Starting AI Review for PR #{PR_NUMBER} in {REPO_NAME}...")

    # 1. Fetch Diff
    diff_url = f"https://api.github.com/repos/{REPO_NAME}/pulls/{PR_NUMBER}"
    try:
        diff_resp = requests.get(diff_url, headers=gh_headers)
        diff_resp.raise_for_status()
        diff_text = diff_resp.text
    except Exception as e:
        print(f"Error fetching diff: {e}")
        return

    if len(diff_text) < 10:
        print("Diff is empty or too short.")
        return

    # 2. Ask LiteLLM
    # Truncate to avoid massive payloads
    if len(diff_text) > 15000:
        diff_text = diff_text[:15000] + "\n...(Truncated)..."

    prompt = f"""
    You are a Senior Code Reviewer. Review this GitHub Pull Request Diff.
    Identify bugs, security issues, or improvements.
    Format as Markdown. Be concise.

    DIFF:
    {diff_text}
    """

    payload = {
        "model": "gpt-4o",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2
    }

    try:
        ai_resp = requests.post(
            f"{LITELLM_BASE_URL}/v1/chat/completions",
            json=payload,
            headers=ai_headers
        )
        ai_resp.raise_for_status()
        review_body = ai_resp.json()['choices'][0]['message']['content']
    except Exception as e:
        print(f"Error calling AI: {e}")
        return

    # 3. Post Comment
    comment_url = f"https://api.github.com/repos/{REPO_NAME}/issues/{PR_NUMBER}/comments"
    post_headers = gh_headers.copy()
    post_headers["Accept"] = "application/vnd.github+json"

    requests.post(comment_url, headers=post_headers, json={"body": f"## 🤖 AI Review\n\n{review_body}"})
    print("Comment posted!")

if __name__ == "__main__":
    main()
