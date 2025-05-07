from django.shortcuts import render
import requests, json
from .utils.pdf_reader import load_and_chunk_pdfs
from django.http import JsonResponse

from django.http import JsonResponse
import requests
import json
from django.views.decorators.csrf import csrf_exempt

# Loaded once at server start
PDF_CHUNKS = load_and_chunk_pdfs("pdfs/")

@csrf_exempt
def LoginPage(request):
    if request.method == "POST":
        try:
            data = json.loads(request.body.decode('utf-8'))
            user_question = data.get("question", "").strip()
        except json.JSONDecodeError:
            return JsonResponse({"error": "Invalid JSON format."}, status=400)

        if not user_question:
            return JsonResponse({"error": "Question is required."}, status=400)

        # Search top 10 relevant chunks
        relevant_chunks = [
            chunk for chunk in PDF_CHUNKS
            if any(word in chunk.lower() for word in user_question.lower().split())
        ][:10]

        context_text = "\n\n".join(relevant_chunks)

        payload = {
            "model": "openai/gpt-4o",
            "messages": [
                {"role": "system", "content": "You are a helpful assistant. Use the following PDF content to answer questions."},
                {"role": "user", "content": f"PDF Content:\n{context_text}\n\nQuestion: {user_question}"}
            ],
            "max_tokens": 512
        }

        try:
            response = requests.post(
                url="https://openrouter.ai/api/v1/chat/completions",
                headers={
                    "Authorization": "Bearer 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",
                    "Content-Type": "application/json",
                    "HTTP-Referer": "https://yourdomain.com",
                    "X-Title": "PDF QnA App"
                },
                data=json.dumps(payload)
            )

            result = response.json()
            if "choices" in result and result["choices"]:
                reply = result["choices"][0]["message"]["content"]
            else:
                reply = result.get("error", {}).get("message", "No valid reply received from API.")

        except Exception as e:
            return JsonResponse({"error": str(e)}, status=500)

        return JsonResponse({"reply": reply})

    return JsonResponse({"error": "Only POST method is allowed"}, status=405)