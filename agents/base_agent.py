"""Base agent class for all agents in the system."""
from abc import ABC, abstractmethod
from typing import Any, Dict
from groq import Groq
from config.settings import config

class BaseAgent(ABC):
    """Abstract base class for all agents."""
    
    def __init__(self):
        self.client = Groq(api_key=config.groq_api_key)
        self.model = config.groq_model
        self.temperature = config.groq_temperature
    
    @abstractmethod
    def process(self, input_data: Any) -> Any:
        """Process input and return result."""
        pass
    
    def _call_llm(self, prompt: str, system_prompt: str = None) -> str:
        """Make a call to Groq LLM."""
        messages = []
        
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        
        messages.append({"role": "user", "content": prompt})
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=self.temperature
        )
        
        return response.choices[0].message.content
