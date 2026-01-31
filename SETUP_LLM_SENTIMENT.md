# OpenAI and Gemini API Keys Setup

Add these environment variables to your `.env` file:

```bash
# OpenAI API (Primary sentiment analyzer)
OPENAI_API_KEY=sk-proj-...

# Google Gemini API (Backup sentiment analyzer)  
GEMINI_API_KEY=AIza...
```

## Install Required Dependencies

```bash
pip install openai google-generativeai
```

## Test the Integration

```bash
cd /home/pretradify/aksyo-wiz-capital
python3 test_llm_sentiment.py
```

> **Note**: The system will work without API keys by falling back to keyword-based analysis, but LLM analysis provides much better accuracy.
