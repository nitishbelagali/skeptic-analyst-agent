import re
from typing import Literal

# Define the valid intents
IntentType = Literal["audit_only", "clean_data", "data_engineer"]

class IntentRouter:
    """Classifies user intent and routes to appropriate workflow."""
    
    def __init__(self):
        self.intent_patterns = {
            "audit_only": [
                r"(just|only|quick).*(audit|check|validate|scan)",
                r"show.*errors",
                r"what.*wrong",
                r"report.*issues",
                r"quality.*check"
            ],
            "clean_data": [
                r"(clean|fix|repair|sanitize)",
                r"remove.*(nulls|duplicates|errors)",
                r"prepare.*data",
                r"get.*ready",
                r"data.*surgeon"
            ],
            "data_engineer": [
                # Broaden the catch for questions
                r"(what is|how many|which).*(average|max|min|count|sum)",
                r"(analyze|insight|trend|pattern|chart|plot|graph)",
                r"(model|star schema|dimension|fact|warehouse)",
                r"(transform|etl|pipeline)",
                r"answer.*question",
                r"build.*database"
            ]
        }
    
    def classify_intent(self, user_message: str) -> IntentType:
        """
        Analyzes user's message to determine workflow.
        """
        msg_lower = user_message.lower()
        scores = {intent: 0 for intent in self.intent_patterns}
        
        for intent, patterns in self.intent_patterns.items():
            for pattern in patterns:
                if re.search(pattern, msg_lower):
                    scores[intent] += 1
        
        # Priority: Engineer > Clean > Audit
        max_score = max(scores.values())
        
        if max_score == 0:
            # If the user asks a specific data question that didn't match keywords, 
            # assume Engineer mode if it looks like a question (?)
            if "?" in user_message:
                return "data_engineer"
            return "audit_only"
        
        if scores["data_engineer"] == max_score: return "data_engineer"
        elif scores["clean_data"] == max_score: return "clean_data"
        else: return "audit_only"
    
    def get_workflow_description(self, intent: IntentType) -> str:
        if intent == "audit_only":
            return "🔍 **AUDIT MODE**: Scanning for errors. No changes."
        elif intent == "clean_data":
            return "🔧 **SURGEON MODE**: Interactive cleaning."
        elif intent == "data_engineer":
            return "🏗️ **ENGINEER MODE**: Pipeline activated (Clean -> Model -> Query)."
        return ""

router = IntentRouter()