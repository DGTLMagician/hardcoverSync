# HardcoverSync AI Roadmap

This document outlines strategic ideas for expanding `hardcoverSync` using the local OpenAI-compatible LLM integration (like Ollama). Since the infrastructure for the LLM is already present in `generate_ai_suggestions`, it can be extended to power several new automated library management features.

## 1. Automated Book Reviews from Annotations
**Concept**: Kobo eReaders store user highlights and annotations. Since we are already reading the Kobo database for reading progress, we can also extract these annotations.
**Execution**:
- When a book is marked as `Finished`/`Read`, extract all highlights and notes for that specific book from `KoboReader.sqlite`.
- Send these notes to the local LLM with a prompt: *"You are an avid reader. Based on my following highlights and notes, draft a thoughtful 2-paragraph book review in my voice."*
- Optionally, automatically push this review to Hardcover.app via the GraphQL API (`insert_user_book_review` mutation).

## 2. AI Auto-Tagging & Genre Organization
**Concept**: Calibre Web Automated (CWA) relies on publisher metadata, which is often messy, overly broad, or inconsistent.
**Execution**:
- When a new book is added to CWA (or downloaded via Shelfmark), send the book's title, author, and description/summary to the LLM.
- Ask the LLM to categorize the book into a strict, predefined set of top-level genres (e.g., `Sci-Fi`, `High Fantasy`, `Thriller`, `Biography`).
- Use the `update_cwa_book_status` logic to write these clean, standardized tags directly back into the Calibre `metadata.db`.

## 3. Conversational Library Assistant
**Concept**: Add a chat interface to the Flask web dashboard.
**Execution**:
- Pass a JSON representation of the user's "Want to Read" list and local CWA library as context to the LLM.
- The user can ask: *"What should I read next if I want a fast-paced thriller that I already have downloaded?"*
- The LLM crosses the user's preference with the local `cwa_books` list and responds with recommendations that are ready to read immediately.

## 4. "Next in Series" Priority
**Concept**: Combine the Series completion check with AI.
**Execution**:
- Use the LLM to analyze reading velocity (e.g., you finished Book 1 and 2 in a week).
- Automatically boost priority or download Book 3 via Shelfmark if the LLM detects high engagement with a specific series, ensuring it is ready on the Kobo before the user even finishes the current book.
