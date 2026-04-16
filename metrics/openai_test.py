import os
from openai import OpenAI
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variable
api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    raise ValueError("OPENAI_API_KEY environment variable not set. Please set it before running this script.")

client = OpenAI(api_key=api_key)


def simple_query():
    try:
        # Sending the request
        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "user",
                 "content": "Write a PySpark function to remove duplicates from a 100M row dataframe based on a timestamp column."}
            ]
        )

        # Printing the result
        print("--- AI Response ---")
        print(completion.choices[0].message.content)

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    simple_query()