from groq import Groq

client = Groq()

# Use llama-3.3-70b-versatile (the current production model)
response = client.chat.completions.create(
    model="llama-3.3-70b-versatile",  # ✅ This is active
    messages=[
        {"role": "user", "content": "Reply with only: OK"}
    ],
    max_tokens=10
)

print(response.choices[0].message.content)
