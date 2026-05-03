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

## 5. Dynamic Reading Moods
**Concept**: Curate the library based on "vibes" or seasons.
**Execution**:
- Analyze the user's recently finished books and Hardcover "currently reading" list.
- Use the LLM to identify the "mood" (e.g., "Fast-paced Tech Noir" or "Atmospheric Gothic Horror").
- Suggest a "Reading Season" theme and auto-curate a list of 5 books from the "Want to Read" shelf that fit this vibe, optionally pre-downloading them via Shelfmark.

## 6. AI-Generated Personalized Challenges
**Concept**: Create gamified reading goals that aren't just "number of books."
**Execution**:
- Ask the LLM to analyze the library and create a custom challenge, e.g., *"The Nebula Winners Sprint: Read 3 Hugo/Nebula award winners you own."*
- Track progress automatically using the CWA/Hardcover sync state and display a custom progress bar in the dashboard.

## 7. "Where You Left Off" Summaries
**Concept**: Help readers get back into books they haven't touched in a while.
**Execution**:
- If a book has been in "Currently Reading" for >2 weeks without progress change on Kobo:
- The system extracts the text surrounding the current progress point (from the EPUB file in CWA).
- The LLM generates a brief "Recap of recent events" to refresh the user's memory when they next pick up their Kobo.

## 8. Theme-Based Library Assistant
**Concept**: Deep-dive into library metadata for complex queries.
**Execution**:
- The user asks: *"Find me something like Dune but with more focus on biology and less on politics."*
- The AI performs a semantic search across book descriptions in the local library and Hardcover data to find the perfect niche match.
