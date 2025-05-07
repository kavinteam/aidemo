import os
from PyPDF2 import PdfReader

import pdfplumber

def load_and_chunk_pdfs(pdf_folder, chunk_size=500):
    all_chunks = []
    for filename in os.listdir(pdf_folder):
        if filename.endswith(".pdf"):
            print(f"\nReading file: {filename}")
            file_path = os.path.join(pdf_folder, filename)
            full_text = ""

            with pdfplumber.open(file_path) as pdf:
                for i, page in enumerate(pdf.pages):
                    text = page.extract_text()
                    if text:
                        print(f"  ✅ Page {i + 1} has text.")
                        full_text += text + "\n"
                    else:
                        print(f"  ⚠️  Page {i + 1} is empty or unreadable!")

            for i in range(0, len(full_text), chunk_size):
                all_chunks.append(full_text[i:i+chunk_size])
                print(f"  ✅ Chunk {len(all_chunks)} from {filename}")
    return all_chunks
