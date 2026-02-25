"""
Minimal test: one API call to LM Studio. Run from project root:
  PYTHONPATH=. python scripts/test_lm_studio.py
"""
from openai import OpenAI

LM_STUDIO_BASE_URL = "http://127.0.0.1:1234/v1"  # LM Studio expects /v1/chat/completions
LM_STUDIO_API_KEY = "sk-lm-Wp2H8t22:wWEOMFLZ1V4C5DXjRtQN" 

def main():
    print("Calling LM Studio at", LM_STUDIO_BASE_URL , "...")
    client = OpenAI(base_url=LM_STUDIO_BASE_URL, api_key= LM_STUDIO_API_KEY )

    try:
        response = client.chat.completions.create(
            model="llama-3.2-3b-instruct",  # must match the model name in LM Studio
            messages=[{"role": "user", "content": "Reply with exactly: Hello from LM Studio"}],
            max_tokens=50,
        )
        content = response.choices[0].message.content
        print("SUCCESS. Model reply:", repr(content))
    except Exception as e:
        print("FAILED:", type(e).__name__, "-", e)

if __name__ == "__main__":
    main()
