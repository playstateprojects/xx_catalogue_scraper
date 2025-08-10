import os
import base64
 
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()


 
client = OpenAI(
    api_key = os.environ.get("MOONSHOT_KEY"), 
    base_url = "https://api.moonshot.ai/v1",
)
 

model_list = client.models.list()
model_data = model_list.data
 
for i, model in enumerate(model_data):
    print(f"model[{i}]:", model.id)